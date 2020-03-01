// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.8

package xorm

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	phoenixormcore "github.com/yongjacky/phoenix-go-orm-core"
)

const (
	// Version show the xorm's version
	Version string = "0.8.0.1015"
)

func regDrvsNDialects() bool {
	providedDrvsNDialects := map[string]struct {
		dbType     phoenixormcore.DbType
		getDriver  func() phoenixormcore.Driver
		getDialect func() phoenixormcore.Dialect
	}{
		"mssql":    {"mssql", func() phoenixormcore.Driver { return &odbcDriver{} }, func() phoenixormcore.Dialect { return &mssql{} }},
		"odbc":     {"mssql", func() phoenixormcore.Driver { return &odbcDriver{} }, func() phoenixormcore.Dialect { return &mssql{} }}, // !nashtsai! TODO change this when supporting MS Access
		"mysql":    {"mysql", func() phoenixormcore.Driver { return &mysqlDriver{} }, func() phoenixormcore.Dialect { return &mysql{} }},
		"mymysql":  {"mysql", func() phoenixormcore.Driver { return &mymysqlDriver{} }, func() phoenixormcore.Dialect { return &mysql{} }},
		"postgres": {"postgres", func() phoenixormcore.Driver { return &pqDriver{} }, func() phoenixormcore.Dialect { return &postgres{} }},
		"pgx":      {"postgres", func() phoenixormcore.Driver { return &pqDriverPgx{} }, func() phoenixormcore.Dialect { return &postgres{} }},
		"sqlite3":  {"sqlite3", func() phoenixormcore.Driver { return &sqlite3Driver{} }, func() phoenixormcore.Dialect { return &sqlite3{} }},
		"oci8":     {"oracle", func() phoenixormcore.Driver { return &oci8Driver{} }, func() phoenixormcore.Dialect { return &oracle{} }},
		"goracle":  {"oracle", func() phoenixormcore.Driver { return &goracleDriver{} }, func() phoenixormcore.Dialect { return &oracle{} }},
	}

	for driverName, v := range providedDrvsNDialects {
		if driver := phoenixormcore.QueryDriver(driverName); driver == nil {
			phoenixormcore.RegisterDriver(driverName, v.getDriver())
			phoenixormcore.RegisterDialect(v.dbType, v.getDialect)
		}
	}
	return true
}

func close(engine *Engine) {
	engine.Close()
}

func init() {
	regDrvsNDialects()
}

// NewEngine new a db manager according to the parameter. Currently support four
// drivers
func NewEngine(driverName string, dataSourceName string) (*Engine, error) {
	driver := phoenixormcore.QueryDriver(driverName)
	if driver == nil {
		return nil, fmt.Errorf("Unsupported driver name: %v", driverName)
	}

	uri, err := driver.Parse(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	dialect := phoenixormcore.QueryDialect(uri.DbType)
	if dialect == nil {
		return nil, fmt.Errorf("Unsupported dialect type: %v", uri.DbType)
	}

	db, err := phoenixormcore.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	err = dialect.Init(db, uri, driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		db:             db,
		dialect:        dialect,
		Tables:         make(map[reflect.Type]*phoenixormcore.Table),
		mutex:          &sync.RWMutex{},
		TagIdentifier:  "xorm",
		TZLocation:     time.Local,
		tagHandlers:    defaultTagHandlers,
		cachers:        make(map[string]phoenixormcore.Cacher),
		defaultContext: context.Background(),
	}

	if uri.DbType == phoenixormcore.SQLITE {
		engine.DatabaseTZ = time.UTC
	} else {
		engine.DatabaseTZ = time.Local
	}

	logger := NewSimpleLogger(os.Stdout)
	logger.SetLevel(phoenixormcore.LOG_INFO)
	engine.SetLogger(logger)
	engine.SetMapper(phoenixormcore.NewCacheMapper(new(phoenixormcore.SnakeMapper)))

	runtime.SetFinalizer(engine, close)

	return engine, nil
}

// NewEngineWithParams new a db manager with params. The params will be passed to dialect.
func NewEngineWithParams(driverName string, dataSourceName string, params map[string]string) (*Engine, error) {
	engine, err := NewEngine(driverName, dataSourceName)
	engine.dialect.SetParams(params)
	return engine, err
}

// Clone clone an engine
func (engine *Engine) Clone() (*Engine, error) {
	return NewEngine(engine.DriverName(), engine.DataSourceName())
}
