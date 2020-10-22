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

package xgrpc

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/douyu/jupiter/pkg/constant"
	"github.com/douyu/jupiter/pkg/ecode"
	"github.com/douyu/jupiter/pkg/server"
	"github.com/douyu/jupiter/pkg/xlog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	"google.golang.org/grpc"
)

// Server ...
type Server struct {
	*grpc.Server
	listener        net.Listener
	gatewayListener net.Listener
	gatewayMux      *runtime.ServeMux
	*Config
}

func newServer(config *Config) *Server {
	var s Server
	s.Config = config
	var streamInterceptors = append(
		[]grpc.StreamServerInterceptor{defaultStreamServerInterceptor(config.logger, config.SlowQueryThresholdInMilli)},
		config.streamInterceptors...,
	)

	var unaryInterceptors = append(
		[]grpc.UnaryServerInterceptor{defaultUnaryServerInterceptor(config.logger, config.SlowQueryThresholdInMilli)},
		config.unaryInterceptors...,
	)

	config.serverOptions = append(config.serverOptions,
		grpc.StreamInterceptor(StreamInterceptorChain(streamInterceptors...)),
		grpc.UnaryInterceptor(UnaryInterceptorChain(unaryInterceptors...)),
	)

	newServer := grpc.NewServer(config.serverOptions...)
	s.Server = newServer

	listener, err := net.Listen(config.Network, config.Address())
	if err != nil {
		config.logger.Panic("new grpc server err", xlog.FieldErrKind(ecode.ErrKindListenErr), xlog.FieldErr(err))
	}
	config.Port = listener.Addr().(*net.TCPAddr).Port
	s.listener = listener

	if config.gatewayRegister != nil && config.GatewayPort != 0 {
		gwListener, err := net.Listen(config.Network, config.GwAddress())
		if err != nil {
			config.logger.Panic("new grpc gateway server err", xlog.FieldErrKind(ecode.ErrKindListenErr), xlog.FieldErr(err))
		}

		var mux = runtime.NewServeMux()
		err = config.gatewayRegister(context.Background(), mux, config.Address(), []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			config.logger.Panic("register grpc gateway server err", xlog.FieldErrKind(ecode.ErrKindListenErr), xlog.FieldErr(err))
		}

		config.GatewayPort = gwListener.Addr().(*net.TCPAddr).Port
		s.gatewayListener = gwListener
		s.gatewayMux = mux
	}

	return &s
}

// Server implements server.Server interface.
func (s *Server) Serve() error {
	var errChan = make(chan error, 1)
	if s.gatewayListener != nil {
		go func() {
			errChan <- http.Serve(s.gatewayListener, s.gatewayMux)
		}()
	}

	go func() {
		errChan <- s.Server.Serve(s.listener)
	}()

	return <-errChan
}

// Stop implements server.Server interface
// it will terminate echo server immediately
func (s *Server) Stop() error {
	s.Server.Stop()
	return nil
}

// GracefulStop implements server.Server interface
// it will stop echo server gracefully
func (s *Server) GracefulStop(ctx context.Context) error {
	s.Server.GracefulStop()
	return nil
}

// Info returns server info, used by governor and consumer balancer
func (s *Server) Info() *server.ServiceInfo {
	serviceAddress := s.listener.Addr().String()
	if s.Config.ServiceAddress != "" {
		serviceAddress = s.Config.ServiceAddress
	}

	var gwAddress string
	if s.gatewayListener != nil {
		gwAddress = s.gatewayListener.Addr().String()
		serviceAddress = fmt.Sprintf("%s,gateway://%s", serviceAddress, gwAddress)
	}

	info := server.ApplyOptions(
		server.WithScheme("grpc"),
		server.WithAddress(serviceAddress),
		server.WithKind(constant.ServiceProvider),
	)
	return &info
}
