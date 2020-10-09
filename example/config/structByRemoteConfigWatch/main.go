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
	"time"

	"aex_jupiter/pkg/conf"
	"aex_jupiter/pkg/server/xecho"
	"aex_jupiter/pkg/xlog"

	"github.com/douyu/jupiter"
)

//  go run main.go --config="http://10.1.42.16:60814/api/v1/agent/config?name=douyu-wsd-shirou&env=dev&target=config-dev-test.toml&port=8023" --watch
// 获取是否发生变化 可通过治理接口  0.0.0.0:9999/configs 获取更新的配置信息，如果长轮询期间没有发生变化，则第一次获取的结果为空
// config 传送参数
// name 应用名称
// env  配置环境
// target 配置名称
// port
func main() {
	app := NewEngine()
	if err := app.Run(); err != nil {
		panic(err)
	}
}

type Engine struct {
	jupiter.Application
}

type People struct {
	Name string
}

func NewEngine() *Engine {
	eng := &Engine{}
	if err := eng.Startup(
		eng.remoteConfigWatch,
		eng.serveHTTP,
	); err != nil {
		xlog.Panic("startup", xlog.Any("err", err))
	}

	return eng
}

func (eng *Engine) serveHTTP() error {
	server := xecho.StdConfig("http").Build()
	return eng.Serve(server)
}

func (eng *Engine) remoteConfigWatch() error {
	p := People{}
	conf.OnChange(func(config *conf.Configuration) {
		err := config.UnmarshalKey("people", &p)
		if err != nil {
			panic(err.Error())
		}
	})
	go func() {
		// 循环打印配置
		for {
			time.Sleep(10 * time.Second)
			xlog.Info("people info", xlog.String("name", p.Name), xlog.String("type", "structByFileWatch"))
		}
	}()
	return nil
}
