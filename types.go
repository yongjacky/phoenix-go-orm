// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"reflect"

	phoenixormcore "github.com/yongjacky/phoenix-go-orm-core"
)

var (
	ptrPkType = reflect.TypeOf(&phoenixormcore.PK{})
	pkType    = reflect.TypeOf(phoenixormcore.PK{})
)
