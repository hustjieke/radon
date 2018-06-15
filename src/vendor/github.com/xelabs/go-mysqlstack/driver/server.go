/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"net"
	"runtime"
	"runtime/debug"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Handler interface.
type Handler interface {
	// NewSession is called when a session is coming.
	NewSession(session *Session)

	// SessionClosed is called when a session exit.
	SessionClosed(session *Session)

	// Check the session.
	SessionCheck(session *Session) error

	// Check the Auth request.
	AuthCheck(session *Session) error

	// Handle the cominitdb.
	ComInitDB(session *Session, database string) error

	// Handle the queries.
	ComQuery(session *Session, query string, callback func(*sqltypes.Result) error) error
}

// Listener is a connection handler.
type Listener struct {
	// Logger.
	log *xlog.Log

	address string

	// Query handler.
	handler Handler

	// This is the main listener socket.
	listener net.Listener

	// Incrementing ID for connection id.
	connectionID uint32
}

// NewListener creates a new Listener.
func NewListener(log *xlog.Log, address string, handler Handler) (*Listener, error) {
	log.Info("gry--监听endpoint(address): %+v", address)
	listener, err := net.Listen("tcp", address) // 比如本机的就是":3308"
	if err != nil {
		return nil, err
	}

	return &Listener{
		log:          log,
		address:      address,
		handler:      handler, // 封装的spanner
		listener:     listener,
		connectionID: 1,
	}, nil
}

// Accept runs an accept loop until the listener is closed.
func (l *Listener) Accept() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for {
		conn, err := l.listener.Accept() // 封装Accept
		if err != nil {
			// Close() was probably called.
			return
		}
		ID := l.connectionID
		l.connectionID++
		go l.handle(conn, ID)
	}
}

func (l *Listener) parserComInitDB(data []byte) string {
	return string(data[1:])
}

func (l *Listener) parserComQuery(data []byte) string {
	log := l.log
	// Trim the right.
	data = data[1:]
	last := len(data) - 1
	log.Info("gry----query length: %+v", last+1)
	if data[last] == ';' {
		log.Info("gry----query has ';'")
		data = data[:last]
	}
	return common.BytesToString(data)
}

// handle is called in a go routine for each client connection.
func (l *Listener) handle(conn net.Conn, ID uint32) {
	var err error
	var data []byte
	var authPkt []byte
	var greetingPkt []byte
	log := l.log

	// Catch panics, and close the connection in any case.
	defer func() {
		conn.Close()
		if x := recover(); x != nil {
			log.Error("server.handle.panic:\n%v\n%s", x, debug.Stack())
		}
	}()
	session := newSession(log, ID, conn)
	// Session check.检查是否达到最大连接数,是否本机,以及ip列表检查
	if err = l.handler.SessionCheck(session); err != nil {
		log.Warning("session[%v].check.failed.error:%+v", ID, err)
		session.writeErrFromError(err)
		return
	}

	// Session register.前面检查通过,则将此新session注册到spanner
	l.handler.NewSession(session) // 用addNewSession(session)更形象一点
	defer l.handler.SessionClosed(session)

	// Greeting packet. 打包,这边应该就是看到的登入界面招呼了,这里是radon和连接者的交互,还不是和mysql
	// 上面理解的不对,这里就是打包greeting信息,然后写入到conn对应的socket文件
	greetingPkt = session.greeting.Pack()
	// 写入,看上面newSession封装的conn,packets包是绑定conn的网络数据包(stream),这里是往mysqlcli写,
	// 就是我们能见到的greeting界面
	if err = session.packets.Write(greetingPkt); err != nil {
		log.Error("server.write.greeting.packet.error: %v", err)
		return
	}

	// Auth packet. 获取packets到Next stream,获取认证信息,上面是发,这里是响应
	// 这里的Next()封装的就是read,跟上面的写对应
	if authPkt, err = session.packets.Next(); err != nil {
		log.Error("server.read.auth.packet.error: %v", err)
		return
	}

	str := common.BytesToString(authPkt)
	log.Info("gry----auth.Unpack之前，authPkt: %+v", str)
	// 这里UnPack出了user信息,整个auth结构体的信息,推测是mysql客户传递接收到的mysql -utest -ptest -P3308中用户"test"和密码"test"
	// 这就是代理的作用,mysql认为radon是mysql-server
	if err = session.auth.UnPack(authPkt); err != nil {
		log.Error("server.unpack.auth.error: %v", err)
		return
	}
	log.Info("gry---session.auth.UnPack之后: %+v", session.auth)

	//  Auth check.
	if err = l.handler.AuthCheck(session); err != nil {
		log.Warning("server.user[%+v].auth.check.failed", session.User())
		session.writeErrFromError(err)
		return
	}

	// Check the database.
	db := session.auth.Database()
	log.Info("gry---Check the database, db: %+v", db)
	if db != "" {
		if err = l.handler.ComInitDB(session, db); err != nil {
			log.Error("server.cominitdb[%s].error:%+v", db, err)
			session.writeErrFromError(err)
			return
		}
		session.SetSchema(db)
	}

	// 如果是use db, mysqlcli session会先发SELECT DATABASE(),再是use gry_db,所以要走好几次
	// mysqlcli接到返回点result之后不做处理,继续执行use gry_db,所以界面是看不到select的结果,
	// 之后，再执行show databases
	if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
		return
	}

	for {
		log.Info("gry--:for loop")
		// Reset packet sequence ID.
		session.packets.ResetSeq()
		if data, err = session.packets.Next(); err != nil {
			return
		}

		switch data[0] {
		case sqldb.COM_QUIT:
			log.Info("gry: sqldb.COM_QUIT")
			return
		case sqldb.COM_INIT_DB:
			log.Info("gry: sqldb.COM_INIT_DB")
			db := l.parserComInitDB(data)
			log.Info("gry: db: %+v", db)
			if err = l.handler.ComInitDB(session, db); err != nil {
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			} else {
				log.Info("gry--session.SetSchema(%+v)", db)
				session.SetSchema(db)
				if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
					return
				}
			}
		case sqldb.COM_PING:
			log.Info("gry: sqldb.COM_PING")
			if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
				return
			}
		case sqldb.COM_QUERY:
			query := l.parserComQuery(data)
			log.Info("gry: sqldb.COM_QUER, query: %+v", query)
			if err = l.handler.ComQuery(session, query, func(qr *sqltypes.Result) error {
				return session.writeResult(qr)
			}); err != nil {
				log.Error("server.handle.query.from.session[%v].error:%+v.query[%s]", ID, err, query)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
				continue
			}
		// case sqldb.COM_FIELD_LIST: // e.g. show columns from table_xxx;
		//continue
		default:
			// add by gry
			query := l.parserComQuery(data)
			cmd := sqldb.CommandString(data[0])
			log.Error("session.command:%s.not.implemented", cmd)
			log.Error("gry---query: %s", query)
			sqlErr := sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "command handling not implemented yet: %s", cmd)
			if err := session.writeErrFromError(sqlErr); err != nil {
				return
			}
		}
		// Reset packet sequence ID.
		session.packets.ResetSeq()
	}
}

// Addr returns the client address.
func (l *Listener) Addr() string {
	return l.address
}

// Close close the listener and all connections.
func (l *Listener) Close() {
	l.listener.Close()
}
