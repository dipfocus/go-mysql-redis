package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"gopkg.in/birkirb/loggers.v1/log"
	"github.com/gomodule/redigo/redis"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	log.Debugf("OnRotate scheduled, log name %s, pos %d", pos.Name, pos.Pos)
	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	log.Infof("OnTableChanged scheduled, database name %s, table name %s", schema, table)
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	log.Debugf("OnDDL scheduled, log name %s, pos %d", nextPos.Name, nextPos.Pos)
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	log.Debugf("OnXID scheduled, log name %s, pos %d", nextPos.Name, nextPos.Pos)
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	// log.Infof("OnRow scheduled, database name %s, table name %s", e.Table.Schema, e.Table.Name)
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		log.Warnf("rule not found, ignore RowsEvent, db name %s, table name %s", e.Table.Schema, e.Table.Name)
		return nil
	}

	var err error
	switch e.Action {
	case canal.InsertAction:
		err = h.r.insertRows(rule, e.Rows)
	case canal.DeleteAction:
		err = h.r.deleteRows(rule, e.Rows)
	case canal.UpdateAction:
		err = h.r.updateRows(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		log.Errorf("sync err %v after binlog %s, close sync", err, h.r.canal.SyncedPosition())
		return errors.Errorf("%s redis err %v, close sync", e.Action, err)
	}


	return h.r.ctx.Err() // FIXME
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "RedisRiverEventHandler"
}

func (r *River) syncLoop() {

	defer r.wg.Done()

	lastSavedTime := time.Now()

	var pos mysql.Position

	for {
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needSavePos = true
					pos = v.pos
				}
			default:
				log.Errorf("invalid event type")
			}
		case <-r.ctx.Done():
			return
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

func (r *River) insertRows(rule *Rule, rows [][]interface{}) error {
	for _, row := range rows {
		if err := r.insertRow(rule, row); err != nil {
			return err
		}
	}
	return nil
}

func (r *River) insertRow(rule *Rule, row []interface{}) error {
	// 获取主键
	pk, err := r.getPKValue(rule, row)
	if err != nil {
		return errors.Trace(err)
	}

	// 获取需要同步的字段value
	values := make(map[string]interface{}, len(row))
	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		values[c.Name] = r.makeReqColumnData(&c, row[i])
	}

	// 写入哈希表
	if _, err := r.redisConn.Do("HMSET", redis.Args{}.Add(pk).AddFlat(values)...); err != nil {
		log.Errorf("sync err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	}

	// 更新统计信息
	r.st.InsertNum.Add(1)

	log.Infof("insert row %s to redis", pk)
	return nil
}

func (r *River) updateRow(rule *Rule, beforeValues []interface{}, afterValues []interface{}) error {
	// 获取主键
	pk, err := r.getPKValue(rule, beforeValues)
	if err != nil {
		return errors.Trace(err)
	}

	// 获取需要同步的字段value
	values := make(map[string]interface{}, len(beforeValues))
	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}

		values[c.Name] = r.makeReqColumnData(&c, afterValues[i])
	}
	// 写入哈希表
	if _, err := r.redisConn.Do("HMSET", redis.Args{}.Add(pk).AddFlat(values)...); err != nil {
		log.Errorf("sync err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	}

	// 更新统计信息
	r.st.UpdateNum.Add(1)
	log.Infof("update row %s to redis", pk)
	return nil
}

func (r *River) deleteRows(rule *Rule, rows [][]interface{}) error {
	for _, row := range rows {
		if err := r.deleteRow(rule, row); err != nil {
			return err
		}
	}

	return nil
}

func (r *River) deleteRow(rule *Rule, row []interface{}) error {
	// 获取主键
	pk, err := r.getPKValue(rule, row)
	if err != nil {
		return errors.Trace(err)
	}

	// 遍历哈希表中key的所有字段，逐个删除
	for _, c := range rule.TableInfo.Columns {
		// FIXME:字段不存在，是否返回错误
		if _, err := r.redisConn.Do("HDEL", pk, c.Name); err != nil {
			log.Errorf("sync err %v after binlog %s", err, r.canal.SyncedPosition())
			return errors.Trace(err)
		}
	}

	// 更新统计信息
	r.st.DeleteNum.Add(1)
	log.Infof("delete row %s from redis", pk)

	return nil
}

func (r *River) updateRows(rule *Rule, rows [][]interface{}) error {
	if len(rows)%2 != 0 {
		return errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	for i := 0; i < len(rows); i += 2 {
		beforePK, err := r.getPKValue(rule, rows[i])
		if err != nil {
			return errors.Trace(err)
		}

		afterPK, err := r.getPKValue(rule, rows[i+1])

		if err != nil {
			return errors.Trace(err)
		}

		if beforePK != afterPK {
			// 删除旧记录
			if err := r.deleteRow(rule, rows[i]); err != nil {
				return errors.Trace(err)
			}

			// 插入新记录
			if err := r.insertRow(rule, rows[i+1]); err != nil {
				return errors.Trace(err)
			}
		} else {
			r.updateRow(rule, rows[i], rows[i+1])

		}

	}

	return nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, _ := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			return vt.Format(time.RFC3339)
		}
	}

	return value
}

/**
func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}
*/

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getPKValue(rule *Rule, row []interface{}) (string, error) {
	var (
		pks []interface{}
		err error
	)

	pks, err = rule.TableInfo.GetPKValues(row)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer

	sep := ":"
	buf.WriteString(fmt.Sprintf("%s%s%s", rule.Schema, sep, rule.Table))

	for i, value := range pks {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
	}

	return buf.String(), nil
}

/**
func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}
*/
/**
// get mysql field value and convert it to specific value to es
func (r *River) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case fieldTypeList:
		v := r.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case fieldTypeDate:
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = r.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = r.makeReqColumnData(col, value)
	}
	return fieldValue
}
*/
