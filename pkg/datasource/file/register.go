package file

import (
	"github.com/achillesss/jupiter/pkg/conf"
	"github.com/achillesss/jupiter/pkg/datasource/manager"
	"github.com/achillesss/jupiter/pkg/flag"
	"github.com/achillesss/jupiter/pkg/xlog"
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
