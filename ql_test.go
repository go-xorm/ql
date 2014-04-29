package ql

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/cznic/ql/driver"
	"github.com/go-xorm/core"
	"github.com/go-xorm/tests"
	"github.com/go-xorm/xorm"
)

var showTestSql = true

func newQlEngine() (*xorm.Engine, error) {
	os.Remove("./ql.db")
	return xorm.NewEngine("ql", "./ql.db")
}

func newQlDriverDB() (*sql.DB, error) {
	os.Remove("./ql.db")
	return sql.Open("ql", "./ql.db")
}

func newCache() core.Cacher {
	return xorm.NewLRUCacher2(xorm.NewMemoryStore(), time.Hour, 1000)
}

func setEngine(engine *xorm.Engine, useCache bool) {
	if useCache {
		engine.SetDefaultCacher(newCache())
	}
	engine.ShowSQL = showTestSql
	engine.ShowErr = showTestSql
	engine.ShowWarn = showTestSql
	engine.ShowDebug = showTestSql
}

func TestQl(t *testing.T) {
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()
	setEngine(engine, false)

	tests.BaseTestAll(engine, t)
	tests.BaseTestAll2(engine, t)
	tests.BaseTestAll3(engine, t)
}

func TestQlWithCache(t *testing.T) {
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	setEngine(engine, true)

	tests.BaseTestAll(engine, t)
	tests.BaseTestAll2(engine, t)
}

const (
	createTableQl = "CREATE TABLE IF NOT EXISTS `big_struct` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `name` TEXT NULL, `title` TEXT NULL, `age` TEXT NULL, `alias` TEXT NULL, `nick_name` TEXT NULL);"
	dropTableQl   = "DROP TABLE IF EXISTS `big_struct`;"
)

func BenchmarkQlDriverInsert(t *testing.B) {
	tests.DoBenchDriver(newQlDriverDB, createTableQl, dropTableQl,
		tests.DoBenchDriverInsert, t)
}

func BenchmarkQlDriverFind(t *testing.B) {
	tests.DoBenchDriver(newQlDriverDB, createTableQl, dropTableQl,
		tests.DoBenchDriverFind, t)
}

func BenchmarkQlNoCacheInsert(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	//engine.ShowSQL = true
	tests.DoBenchInsert(engine, t)
}

func BenchmarkQlNoCacheFind(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	//engine.ShowSQL = true
	tests.DoBenchFind(engine, t)
}

func BenchmarkQlNoCacheFindPtr(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()
	//engine.ShowSQL = true
	tests.DoBenchFindPtr(engine, t)
}

func BenchmarkQlCacheInsert(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	engine.SetDefaultCacher(newCache())
	tests.DoBenchInsert(engine, t)
}

func BenchmarkQlCacheFind(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	engine.SetDefaultCacher(newCache())
	tests.DoBenchFind(engine, t)
}

func BenchmarkQlCacheFindPtr(t *testing.B) {
	t.StopTimer()
	engine, err := newQlEngine()
	if err != nil {
		t.Error(err)
		return
	}
	defer engine.Close()

	engine.SetDefaultCacher(newCache())
	tests.DoBenchFindPtr(engine, t)
}
