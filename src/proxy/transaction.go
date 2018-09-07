/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleBegin used to handle Multi-statement transaction "begin" or "start transaction"
func (spanner *Spanner) handleBegin(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteMultiStatBegin(session, query, node)
}

// handleBegin used to handle Multi-statement transaction "rollback"
func (spanner *Spanner) handleRollback(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteMultiStatRollback(session, query, node)
}

// handleBegin used to handle Multi-statement transaction "commit"
func (spanner *Spanner) handleCommit(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteMultiStatCommit(session, query, node)
}
