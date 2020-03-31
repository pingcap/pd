// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package swaggerserver

import (
	"context"
	"github.com/pingcap/pd/v4/server"
	"net/http"
)

const swaggerPrefix = "/swagger/"

var (
	swaggerServiceGroup = server.ServiceGroup{
		Name:       "swagger",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: swaggerPrefix,
	}
)

// GetServiceBuilders returns all ServiceBuilders required by Swagger
func GetServiceBuilders() []server.HandlerBuilder {
	return []server.HandlerBuilder{
		func(context.Context, *server.Server) (http.Handler, server.ServiceGroup, error) {
			swaggerHandler := http.NewServeMux()
			swaggerHandler.Handle(swaggerPrefix, Handler())
			return swaggerHandler, swaggerServiceGroup, nil
		},
	}
}
