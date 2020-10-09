package file

import (
	"aex_jupiter/pkg/conf"
	"aex_jupiter/pkg/datasource/manager"
	"aex_jupiter/pkg/flag"
	"aex_jupiter/pkg/xlog"
)

// DataSourceFile defines file scheme
const DataSourceFile = "file"

func init() {
	manager.Register(DataSourceFile, func() conf.DataSource {
		var (
			watchConfig = flag.Bool("watch")
			configAddr  = flag.String("config")
		)
		if configAddr == "" {
			xlog.Panic("new file dataSource, configAddr is empty")
			return nil
		}
		return NewDataSource(configAddr, watchConfig)
	})
	manager.DefaultScheme = DataSourceFile
}
