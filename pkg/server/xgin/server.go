// Copyright 2020 Douyu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xgin

import (
	"context"
	"fmt"
	"net/http"

	"github.com/douyu/jupiter/pkg"
	"github.com/douyu/jupiter/pkg/server"
	"github.com/douyu/jupiter/pkg/xlog"
	"github.com/gin-gonic/gin"
)

// Server ...
type Server struct {
	*gin.Engine
	Server *http.Server
	config *Config
}

func newServer(config *Config) *Server {
	return &Server{
		Engine: gin.New(),
		config: config,
	}
}

// Serve implements server.Server interface.
func (s *Server) Serve() error {
	gin.SetMode(s.config.Mode)
	// s.Gin.StdLogger = xlog.JupiterLogger.StdLog()
	for _, route := range s.Engine.Routes() {
		s.config.logger.Info("add route", xlog.FieldMethod(route.Method), xlog.String("path", route.Path))
	}
	s.Server = &http.Server{
		Addr:    s.config.Address(),
		Handler: s,
	}
	err := s.Server.ListenAndServe()
	fmt.Printf("err => %+v\n", err)
	if err == http.ErrServerClosed {
		s.config.logger.Info("close gin", xlog.FieldAddr(s.config.Address()))
		return nil
	}

	return err
}

// Stop implements server.Server interface
// it will terminate gin server immediately
func (s *Server) Stop() error {
	return s.Server.Close()
}

// GracefulStop implements server.Server interface
// it will stop gin server gracefully
func (s *Server) GracefulStop(ctx context.Context) error {
	return s.Server.Shutdown(ctx)
}

// Info returns server info, used by governor and consumer balancer
// TODO(gorexlv): implements government protocol with juno
func (s *Server) Info() *server.ServiceInfo {
	return &server.ServiceInfo{
		Name:      pkg.Name(),
		Scheme:    "http",
		IP:        s.config.Host,
		Port:      s.config.Port,
		Weight:    0.0,
		Enable:    false,
		Healthy:   false,
		Metadata:  map[string]string{},
		Region:    "",
		Zone:      "",
		GroupName: "",
	}
}
