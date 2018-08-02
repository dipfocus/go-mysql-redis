package river

import (
	"github.com/siddontang/go-mysql/schema"
)

// Rule is the rule for how to sync data from MySQL to Redis.
// If you want to sync MySQL data into elasticsearch, you must set a rule to let us know how to do it.
// The mapping rule may this: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string   `toml:"schema"`
	Table  string   `toml:"table"`
	// PK     []string `toml:"pk"`

	// MySQL table information
	TableInfo *schema.Table

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `toml:"filter"`
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table

	return r
}

// CheckFilter checkers whether the field needs to be filtered.
func (r *Rule) CheckFilter(field string) bool {
	if r.Filter == nil {
		return true
	}

	for _, f := range r.Filter {
		if f == field {
			return true
		}
	}
	return false
}
