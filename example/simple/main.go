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

package main

import (
	"log"

	compound_registry "AEX_SERVICE/aex_jupiter/pkg/registry/compound"
	etcdv3_registry "AEX_SERVICE/aex_jupiter/pkg/registry/etcdv3"
	"AEX_SERVICE/aex_jupiter/pkg/server"
	"AEX_SERVICE/aex_jupiter/pkg/server/xgin"

	"github.com/douyu/jupiter"
	"github.com/gin-gonic/gin"
)

func main() {
	app, err := jupiter.New()
	if err != nil {
		log.Fatal(err)
	}
	app.SetRegistry(
		compound_registry.New(
			etcdv3_registry.StdConfig("test").Build(),
		),
	)
	app.Run(startHTTPServer())
}

func startHTTPServer() server.Server {
	server := xgin.DefaultConfig().Build()
	server.GET("/hello", func(ctx *gin.Context) {
		ctx.JSON(200, "hello jupiter")
	})
	return server
}
