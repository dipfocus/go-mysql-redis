package river

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/gomodule/redigo/redis"
)

var myAddr = flag.String("my_addr", "127.0.0.1:3306", "MySQL addr")
var redisAddr = flag.String("redis_addr", "127.0.0.1:6379", "Redis addr")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	c *client.Conn
	r *River
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	// 连接mysql
	var err error
	s.c, err = client.Connect(*myAddr, "root", "", "test")
	c.Assert(err, IsNil)

	// 设置binlog格式为ROW
	s.testExecute(c, "SET GLOBAL binlog_format = 'ROW'")
	// s.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            content VARCHAR(256),
            mylist VARCHAR(256),
            mydate INT(10),
            tenum ENUM("e1", "e2", "e3"),
            tset SET("a", "b", "c"),
            tbit BIT(1) default 1,
			tdatetime DATETIME DEFAULT NULL,
			ip INT UNSIGNED DEFAULT 0,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

    // 创建表
	s.testExecute(c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(c, "DROP TABLE IF EXISTS test_river_filter")
	s.testExecute(c, fmt.Sprintf(schema, "test_river"))
	s.testExecute(c, fmt.Sprintf(schema, "test_river_filter"))

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *myAddr
	cfg.MyUser = "root"
	cfg.MyPassword = ""
	cfg.MyCharset = "utf8"
	cfg.RedisAddr = *redisAddr

	cfg.ServerID = 1001
	cfg.Flavor = "mysql"

	cfg.DataDir = "./test_river"
	cfg.DumpExec = "mysqldump"

	cfg.StatAddr = "127.0.0.1:12800"
	cfg.FlushBulkTime = TomlDuration{3 * time.Millisecond}

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: "test", Tables: []string{"test_river", "test_river_[0-9]{4}", "test_river_filter"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: "test",
			Table:        "test_river",
		},

		&Rule{Schema: "test",
			Table:        "test_river_filter",
		},

		&Rule{Schema: "test",
			Table:        "test_river_[0-9]{4}",
		},
	}

	s.r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	s.testRedisClear(c)
	s.testPrepareData(c)
	go func() { s.r.Run() }()

	testWaitSyncDone(c, s.r)
	fmt.Printf("init env succ\n")
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.c != nil {
		s.c.Close()
	}

	if s.r != nil {
		s.r.Close()
	}
}

func (s *riverTestSuite) TestConfig(c *C) {
	str := `
my_addr = "127.0.0.1:3306"
my_user = "root"
my_pass = ""
my_charset = "utf8"
redis_addr = "127.0.0.1:6379"
data_dir = "./var"

[[source]]
schema = "test"

tables = ["test_river", "test_river_[0-9]{4}", "test_river_filter"]

[[rule]]
schema = "test"
table = "test_river"


[[rule]]
schema = "test"
table = "test_river_filter"


[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"

`

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 3)
	c.Assert(cfg.Rules, HasLen, 3)
}

func (s *riverTestSuite) testExecute(c *C, query string, args ...interface{}) {
	c.Logf("query %s, args: %v", query, args)
	_, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello redis 3", "e3", "c")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset, tbit) VALUES (?, ?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-redis 4", "e1", "a,b,c", 0)
	s.testExecute(c, "INSERT INTO test_river_filter (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}

	datetime := time.Now().Format(mysql.TimeFormat)
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset, tdatetime, mydate) VALUES (?, ?, ?, ?, ?, ?, ?)", 16, "test datetime", "hello go 16", "e1", "a,b", datetime, 1458131094)

	// test ip
	s.testExecute(c, "INSERT test_river (id, ip) VALUES (?, ?)", 17, 0)
}


func (s *riverTestSuite) testRedisGet(c *C, key string) ([]interface{}, error) {

	v, err := redis.Values(s.r.redisConn.Do("HGETALL", key))

	c.Assert(err, IsNil)

	return v, nil
}
func (s *riverTestSuite) testRedisClear(c *C) {

	v, err := redis.Values(s.r.redisConn.Do("KEYS", "test:test_*"))

	c.Assert(err, IsNil)

	for _, key := range v {
		columns, err := redis.Values(s.r.redisConn.Do("HKEYS", key))
		for _, column := range columns {
			_, err = s.r.redisConn.Do("HDEL", key, column)
			c.Assert(err, IsNil)
			// fmt.Printf("delete redis key:%s, column:%s\n", key, column)
		}
	}

	return
}

func testWaitSyncDone(c *C, r *River) {
	<-r.canal.WaitDumpDone()

	err := r.canal.CatchMasterPos(10 * time.Second)
	c.Assert(err, IsNil)

	for i := 0; i < 1000; i++ {
		if len(r.syncCh) == 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	c.Fatalf("wait 1s but still have %d items to be synced", len(r.syncCh))
}


func (s *riverTestSuite) TestInsert(c *C) {
	// s.testPrepareData(c)
	var p1 struct {
		ID int `redis:"id"`
		Title  string `redis:"title"`
		Content string `redis:"content"`
		Mylist   string `redis:"mylist"`
		// Mydate	int `redis:"mydate"`
		Tenum	string `redis:"tenum"`
		Tset	string `redis:"tset"`
	}

	r, err := s.testRedisGet(c, "test:test_river:100")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)

	r, err = s.testRedisGet(c, "test:test_river:1")


	err = redis.ScanStruct(r, &p1)
	c.Assert(err, IsNil)

	// fmt.Printf("%+v\n", p1)
	c.Assert(p1.Tenum, Equals, "e1")
	c.Assert(p1.Tset, Equals, "a,b")
	c.Assert(p1.Title, Equals, "first")
}

/**
func (s *riverTestSuite) TestUpdateColumn(c *C) {
	// s.testPrepareData(c)

	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	// s.testExecute(c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 30, 3)

	time.Sleep(100)
	var p1 struct {
		ID int `redis:"id"`
		Title  string `redis:"title"`
		Content string `redis:"content"`
		Mylist   string `redis:"mylist"`
		// Mydate	int `redis:"mydate"`
		Tenum	string `redis:"tenum"`
		Tset	string `redis:"tset"`
	}

	r, err := s.testRedisGet(c, "test:test_river:2")
	c.Assert(err, IsNil)

	err = redis.ScanStruct(r, &p1)
	c.Assert(err, IsNil)

	// fmt.Printf("%+v\n", p1)
	c.Assert(p1.Title, Equals, "second 2")
	c.Assert(p1.Tenum, Equals, "e3")
	c.Assert(p1.Tset, Equals, "a,b,c")
	c.Assert(p1.Mylist, Equals, "a,b,c")
	/**
	// so we can insert invalid data
	s.testExecute(c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.testExecute(c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	// test ip
	s.testExecute(c, "UPDATE test_river set ip = ? WHERE id = ?", 3748168280, 17)

	testWaitSyncDone(c, s.r)

	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, IsFalse)

	r = s.testElasticGet(c, "2")
	c.Assert(r.Found, IsTrue)
	c.Assert(r.Source["es_title"], Equals, "second 2")
	c.Assert(r.Source["tenum"], Equals, "e3")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["es_mylist"], DeepEquals, []interface{}{"a", "b", "c"})
	c.Assert(r.Source["tbit"], Equals, float64(1))

	r = s.testElasticGet(c, "4")
	c.Assert(r.Found, IsTrue)
	c.Assert(r.Source["tenum"], Equals, "")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["tbit"], Equals, float64(0))

	r = s.testElasticGet(c, "3")
	c.Assert(r.Found, IsFalse)

	r = s.testElasticGet(c, "30")
	c.Assert(r.Found, IsTrue)
	c.Assert(r.Source["es_title"], Equals, "second 30")

	for i := 0; i < 10; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, IsTrue)
		c.Assert(r.Source["es_title"], Equals, "hello")
	}

	// test ip
	r = s.testElasticGet(c, "17")
	c.Assert(r.Found, IsTrue)
	c.Assert(r.Source["ip"], Equals, float64(3748168280))

	// alter table
	s.testExecute(c, "ALTER TABLE test_river ADD COLUMN new INT(10)")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset, new) VALUES (?, ?, ?, ?, ?, ?)", 1000, "abc", "hello", "e1", "a,b,c", 1)
	s.testExecute(c, "ALTER TABLE test_river DROP COLUMN new")
	s.testExecute(c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1001, "abc", "hello", "e1", "a,b,c")

	testWaitSyncDone(c, s.r)

	r = s.testElasticGet(c, "1000")
	c.Assert(r.Found, IsTrue)
	c.Assert(r.Source["new"], Equals, float64(1))

	r = s.testElasticGet(c, "1001")
	c.Assert(r.Found, IsTrue)
	_, ok := r.Source["new"]
	c.Assert(ok, IsFalse)

}
*/
/**
func (s *riverTestSuite) TestDelete(c *C) {
	s.testExecute(c, "DELETE FROM test_river WHERE id = ?", 1)
	time.Sleep(time.Duration(2)*time.Second)
	r, err := s.testRedisGet(c, "test:test_river:1")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}
*/
func TestTableValidation(t *testing.T) {
	tables := []struct {
		Tables []string
		Expect bool
	}{
		{[]string{"*"}, true},
		{[]string{"table", "table2"}, true},
		{[]string{"*", "table"}, false},
	}

	for _, table := range tables {
		if isValidTables(table.Tables) != table.Expect {
			t.Errorf("Tables: %s, Expected: is %t, but: was %t", table.Tables, table.Expect, isValidTables(table.Tables))
		}
	}
}

func TestBuildTable(t *testing.T) {
	tables := []struct {
		Table  string
		Expect string
	}{
		{"*", ".*"},
		{"table2", "table2"},
	}

	for _, table := range tables {
		if buildTable(table.Table) != table.Expect {
			t.Errorf("Table: %s, Expected: is \"%s\", but: was \"%s\"", table.Table, table.Expect, buildTable(table.Table))
		}
	}
}
