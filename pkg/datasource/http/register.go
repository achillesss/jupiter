package http

import (
	"aex_jupiter/pkg/conf"
	"aex_jupiter/pkg/datasource/manager"
	"aex_jupiter/pkg/flag"
	"aex_jupiter/pkg/xlog"
)

// Defines http/https scheme
const (
	DataSourceHttp  = "http"
	DataSourceHttps = "https"
)

func init() {
	dataSourceCreator := func() conf.DataSource {
		var (
			watchConfig = flag.Bool("watch")
			configAddr  = flag.String("config")
		)
		if configAddr == "" {
			xlog.Panic("new http dataSource, configAddr is empty")
			return nil
		}
		return NewDataSource(configAddr, watchConfig)
	}
	manager.Register(DataSourceHttp, dataSourceCreator)
	manager.Register(DataSourceHttps, dataSourceCreator)
}
