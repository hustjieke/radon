/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"fmt"

	"backend"
	"executor"
	"optimizer"
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// ExecuteMultiStatWithTwoPC used to execute multi-transactions with 2pc commit.
func (spanner *Spanner) ExecuteMultiStatWithTwoPC(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	router := spanner.router
	sessions := spanner.sessions

	var err error
	var txn backend.Transaction

	mysession := sessions.getTxnSession(session)
	txn = mysession.transaction
	// binding.
	sessions.MultiStateTxnBinding(session, nil, node, query)
	defer sessions.MultiStateTxnUnBinding(session, false)

	// Transaction execute.
	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}

	executors := executor.NewTree(log, plans, txn)
	qr, err := executors.Execute()
	if err != nil {
		if x := txn.Rollback(); x != nil {
			defer sessions.MultiStateTxnUnBinding(session, true)
			txn.Finish() // if some error happens, execute txn finish
			log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", x)
		}
		return nil, err
	}
	return qr, nil
}

// SingleStatExecuteTwoPC used to execute single statement transaction with 2pc commit.
func (spanner *Spanner) ExecuteSingleStatWithTwoPC(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	var err error
	var txn backend.Transaction

	txn, err = scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()

	// txn limits.
	txn.SetTimeout(conf.Proxy.QueryTimeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)
	switch node.(type) {
	case *sqlparser.Select:
		txn.SetIsSingleStatSelect(true)
	}

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	// Transaction begin.
	if err := txn.Begin(); err != nil {
		log.Error("spanner.execute.2pc.txn.begin.error:[%v]", err)
		return nil, err
	}

	// Transaction execute.
	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}

	executors := executor.NewTree(log, plans, txn)
	qr, err := executors.Execute()
	if err != nil {
		if x := txn.Rollback(); x != nil {
			log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", x)
		}
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		log.Error("spanner.execute.2pc.txn.commit.error:[%v]", err)
		return nil, err
	}
	return qr, nil
}

// ExecuteNormal used to execute non-2pc querys to shards with QueryTimeout limits.
func (spanner *Spanner) ExecuteNormal(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	timeout := spanner.conf.Proxy.QueryTimeout
	return spanner.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteDDL used to execute ddl querys to the shards with DDLTimeout limits, used for create/drop index long time operation.
func (spanner *Spanner) ExecuteDDL(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	spanner.log.Info("spanner.execute.ddl.query:%s", query)
	timeout := spanner.conf.Proxy.DDLTimeout
	return spanner.executeWithTimeout(session, database, query, node, timeout)
}

// ExecuteNormal used to execute non-2pc querys to shards with timeout limits.
// timeout:
//    0x01. if timeout <= 0, no limits.
//    0x02. if timeout > 0, the query will be interrupted if the timeout(in millisecond) is exceeded.
func (spanner *Spanner) executeWithTimeout(session *driver.Session, database string, query string, node sqlparser.Statement, timeout int) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()

	// txn limits.
	txn.SetTimeout(timeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	plans, err := optimizer.NewSimpleOptimizer(log, database, query, node, router).BuildPlanTree()
	if err != nil {
		return nil, err
	}
	executors := executor.NewTree(log, plans, txn)
	qr, err := executors.Execute()
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// ExecuteStreamFetch used to execute a stream fetch query.
func (spanner *Spanner) ExecuteStreamFetch(session *driver.Session, database string, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error, streamBufferSize int) error {
	log := spanner.log
	router := spanner.router
	scatter := spanner.scatter
	sessions := spanner.sessions

	// transaction.
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return err
	}
	defer txn.Finish()

	// binding.
	sessions.TxnBinding(session, txn, node, query)
	defer sessions.TxnUnBinding(session)

	selectNode, ok := node.(*sqlparser.Select)
	if !ok {
		return errors.New("ExecuteStreamFetch.only.support.select")
	}

	plan := planner.NewSelectPlan(log, database, query, selectNode, router)
	if err := plan.Build(); err != nil {
		return err
	}
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.Querys = plan.Querys
	reqCtx.RawQuery = plan.RawQuery
	return txn.ExecuteStreamFetch(reqCtx, callback, streamBufferSize)
}

// Execute used to execute querys to shards.
func (spanner *Spanner) Execute(session *driver.Session, database string, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	// Execute.
	if spanner.isTwoPC() {
		if spanner.IsDML(node) {
			// if single statement transaction or not.
			var txn backend.Transaction
			sessions := spanner.sessions
			mysession := sessions.getTxnSession(session)
			txn = mysession.transaction
			if txn == nil { // single statement transaction.
				return spanner.ExecuteSingleStatWithTwoPC(session, database, query, node)
			}
			return spanner.ExecuteMultiStatWithTwoPC(session, database, query, node)
		}
		return spanner.ExecuteNormal(session, database, query, node)
	}
	return spanner.ExecuteNormal(session, database, query, node)
}

// ExecuteSingle used to execute query on one shard without planner.
// The query must contain the database, such as db.table.
func (spanner *Spanner) ExecuteSingle(query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.single.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteSingle(query)
}

// ExecuteScatter used to execute query on all shards without planner.
func (spanner *Spanner) ExecuteScatter(query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.scatter.txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteScatter(query)
}

// ExecuteOnThisBackend used to executye query on the backend whitout planner.
func (spanner *Spanner) ExecuteOnThisBackend(backend string, query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.execute.on.this.backend..txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteOnThisBackend(backend, query)
}

// ExecuteOnBackup used to executye query on the backup.
func (spanner *Spanner) ExecuteOnBackup(database string, query string) (*sqltypes.Result, error) {
	log := spanner.log
	scatter := spanner.scatter
	txn, err := scatter.CreateBackupTransaction()
	if err != nil {
		log.Error("spanner.execute.on.backup..txn.create.error:[%v]", err)
		return nil, err
	}
	defer txn.Finish()
	return txn.ExecuteRaw(database, query)
}

// ExecuteMultiStatBegin used to execute "start transaction" or "begin".
func (spanner *Spanner) ExecuteMultiStatBegin(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	conf := spanner.conf
	sessions := spanner.sessions
	scatter := spanner.scatter
	var txn backend.Transaction
	var err error

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.2pc.disable")
		qr := &sqltypes.Result{Warnings: 1}
		return qr, fmt.Errorf("spanner.query.execute.multi.transaction.error[twopc-disable]")
	}

	// transaction.
	currentSession := sessions.getTxnSession(session)
	txn = currentSession.transaction

	// If not nil, make sure to commit previous transaction first and then begin another
	// the previous transaction begin without commit or rollback
	// e.g.: begin; sql1; sql2; sql3; begin; sql4; sql5; commit;
	if txn != nil {
		if err := txn.Commit(); err != nil {
			log.Error("spanner.execute.2pc.txn.commit.error:[%v]", err)
			if x := txn.Rollback(); x != nil {
				log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", x)
			}
			txn.Finish()
			sessions.TxnUnBinding(session)
			return nil, err
		}
		txn.Finish()
		sessions.TxnUnBinding(session)
	}

	// create txn
	txn, err = scatter.CreateTransaction()
	if err != nil {
		log.Error("spanner.txn.create.error:[%v]", err)
		return nil, err
	}
	// txn limits.
	txn.SetTimeout(conf.Proxy.QueryTimeout)
	txn.SetMaxResult(conf.Proxy.MaxResultSize)

	// binding.
	sessions.MultiStateTxnBinding(session, txn, node, query)
	defer sessions.MultiStateTxnUnBinding(session, false)
	if err := txn.Begin(); err != nil {
		txn.Finish()
		sessions.TxnUnBinding(session)
		log.Error("spanner.execute.2pc.txn.begin.error:[%v]", err)
		return nil, err
	}
	qr := &sqltypes.Result{Warnings: 1}
	return qr, nil
}

// ExecuteMultiStatRollback used to execute multi-statement transaction sql:"rollback"
func (spanner *Spanner) ExecuteMultiStatRollback(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	sessions := spanner.sessions
	var txn backend.Transaction

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.2pc.disable")
		qr := &sqltypes.Result{Warnings: 1}
		return qr, fmt.Errorf("spanner.query.execute.multi.transaction.error[twopc-disable]")
	}

	// transaction.
	currentSession := sessions.getTxnSession(session)
	txn = currentSession.transaction

	// Nothing to do if "rollback" was send without begin a multi-transaction.
	if txn == nil {
		qr := &sqltypes.Result{Warnings: 1}
		return qr, nil
	}

	defer txn.Finish()
	defer sessions.MultiStateTxnUnBinding(session, true)
	if err := txn.Rollback(); err != nil {
		log.Error("spanner.execute.2pc.error.to.rollback.still.error:[%v]", err)
		return nil, err
	}
	qr := &sqltypes.Result{Warnings: 1}
	return qr, nil
}

// ExecuteMultiStatCommit used to execute multi-statement transaction: "commit"
func (spanner *Spanner) ExecuteMultiStatCommit(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	sessions := spanner.sessions
	var txn backend.Transaction

	if !spanner.isTwoPC() {
		log.Error("spanner.execute.2pc.disable")
		qr := &sqltypes.Result{Warnings: 1}
		return qr, fmt.Errorf("spanner.query.execute.multi.transaction.error[twopc-disable]")
	}

	// transaction.
	currentSession := sessions.getTxnSession(session)
	txn = currentSession.transaction

	// Nothing to do if "commit" was send without begin a multi-transaction.
	if txn == nil {
		qr := &sqltypes.Result{Warnings: 1}
		return qr, nil
	}

	defer txn.Finish()
	defer sessions.MultiStateTxnUnBinding(session, true)
	if err := txn.Commit(); err != nil {
		log.Error("spanner.execute.2pc.txn.commit.error:[%v]", err)
		return nil, err
	}
	qr := &sqltypes.Result{Warnings: 1}
	return qr, nil
}
