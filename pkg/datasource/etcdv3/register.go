package etcdv3

import (
	"AEX_SERVICE/aex_jupiter/pkg/client/etcdv3"
	"AEX_SERVICE/aex_jupiter/pkg/conf"
	"AEX_SERVICE/aex_jupiter/pkg/datasource/manager"
	"AEX_SERVICE/aex_jupiter/pkg/flag"
	"AEX_SERVICE/aex_jupiter/pkg/xlog"
	"net/url"
)

// DataSourceEtcdv3 defines etcdv3 scheme
const DataSourceEtcdv3 = "etcdv3"

func init() {
	manager.Register(DataSourceEtcdv3, func() conf.DataSource {
		var (
			configAddr = flag.String("config")
		)
		if configAddr == "" {
			xlog.Panic("new apollo dataSource, configAddr is empty")
			return nil
		}
		// configAddr is a string in this format:
		// etcdv3://ip:port?basicAuth=true&username=XXX&password=XXX&key=XXX&certFile=XXX&keyFile=XXX&caCert=XXX&secure=XXX

		urlObj, err := url.Parse(configAddr)
		if err != nil {
			xlog.Panic("parse configAddr error", xlog.FieldErr(err))
			return nil
		}
		etcdConf := etcdv3.DefaultConfig()
		etcdConf.Endpoints = []string{urlObj.Host}
		if urlObj.Query().Get("basicAuth") == "true" {
			etcdConf.BasicAuth = true
		}
		if urlObj.Query().Get("secure") == "true" {
			etcdConf.Secure = true
		}
		etcdConf.CertFile = urlObj.Query().Get("certFile")
		etcdConf.KeyFile = urlObj.Query().Get("keyFile")
		etcdConf.CaCert = urlObj.Query().Get("caCert")
		etcdConf.UserName = urlObj.Query().Get("username")
		etcdConf.Password = urlObj.Query().Get("password")
		return NewDataSource(etcdConf.Build(), urlObj.Query().Get("key"))
	})
}
