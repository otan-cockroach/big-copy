package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"strings"
)

var maxRows = flag.Int("max_rows", 1000000, "maximum number of rows to insert")
var jsonSize = flag.Int("json_size", 64*1024*1024, "size of generated JSON blobs")
var dbUrl = flag.String("db", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "db url")

// create table test_table(id int, data json);
func main() {
	flag.Parse()

	ctx := context.Background()
	fmt.Printf("connecting to db\n")
	conn, err := pgx.Connect(ctx, *dbUrl)
	if err != nil {
		panic(errors.Newf("Unable to connect to database: %v", err))
	}
	defer conn.Close(context.Background())

	fmt.Printf("connected to db\n")

	if _, err := conn.Exec(ctx, "TRUNCATE TABLE test_table"); err != nil {
		panic(errors.Wrapf(err, "failed to truncate table"))
	}
	fmt.Printf("table truncated\n")

	type jsonStruct struct {
		str string
	}
	j, err := json.Marshal(&jsonStruct{str: strings.Repeat("a", *jsonSize)})
	if err != nil {
		panic(errors.Wrapf(err, "failing generating json"))
	}
	s := &copyFromSource{
		str: j,
	}
	fmt.Printf("beginning copy process\n")
	r, err := conn.CopyFrom(ctx, pgx.Identifier{"test_table"}, []string{"id", "data"}, s)
	if err != nil {
		panic(errors.Wrapf(err, "failed to copy at %d rows", s.rowsInserted))
	}
	fmt.Printf("copied %d rows\n", r)
}

type copyFromSource struct {
	rowsInserted int
	str          []byte
}

func (c *copyFromSource) Next() bool {
	c.rowsInserted++
	if c.rowsInserted%100 == 0 {
		fmt.Printf("copied %d rows\n", c.rowsInserted)
	}
	return c.rowsInserted < (*maxRows + 1)
}

func (c *copyFromSource) Values() ([]interface{}, error) {
	var ret []interface{}
	ret = append(ret, c.rowsInserted)
	ret = append(ret, c.str)
	return ret, nil
}

func (c *copyFromSource) Err() error {
	return nil
}
