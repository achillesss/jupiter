package http

import (
	"github.com/achillesss/jupiter/pkg/conf"
	"github.com/achillesss/jupiter/pkg/datasource/manager"
	"github.com/achillesss/jupiter/pkg/flag"
	"github.com/achillesss/jupiter/pkg/xlog"
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
