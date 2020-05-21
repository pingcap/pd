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

package uiserver

import (
	"io"
	"net/http"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/pingcap-incubator/tidb-dashboard/pkg/uiserver"
)

var (
	fs = assetFS()
)

// Handler returns an http.Handler that serves the dashboard UI
func Handler() http.Handler {
	if fs == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "Dashboard UI is not built.\n")
		})
	}
	return uiserver.NewGzipHandler(fs)
}

// AssetFS returns the AssetFS of the dashboard UI
func AssetFS() *assetfs.AssetFS {
	return fs
}
