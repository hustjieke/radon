/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
)

func localHostLogin(host string) bool {
	if host == "127.0.0.1" {
		return true
	}
	return false
}

func localUserLogin(s *driver.Session) bool {
	host, _, err := net.SplitHostPort(s.Addr())
	if err != nil {
		return false
	}

	if host == "127.0.0.1" && s.User() == "root" {
		return true
	}
	return false
}

// SessionCheck used to check authentication.
func (spanner *Spanner) SessionCheck(s *driver.Session) error {
	// Max connection check.
	max := spanner.conf.Proxy.MaxConnections
	if spanner.sessions.Reaches(max) {
		return sqldb.NewSQLError(sqldb.ER_CON_COUNT_ERROR, "Too many connections(max: %v)", max)
	}

	log := spanner.log
	host, _, err := net.SplitHostPort(s.Addr())
	if err != nil {
		log.Error("proxy.spanner.split.address.error:%+v", s.Addr())
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user from host '%v'", s.Addr())
	}

	// Local login bypass.
	if localHostLogin(host) {
		return nil
	}

	// Ip check.上面的检查说明,不管本地ip在不在列表,都不受限制
	if !spanner.iptable.Check(host) {
		log.Warning("proxy.spanner.host[%s].denied", host)
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user from host '%v'", host)
	}
	return nil
}

// AuthCheck impl.
func (spanner *Spanner) AuthCheck(s *driver.Session) error {
	log := spanner.log
	log.Info("gry--AuthCheck, localUserLogin检查")
	// Local login bypass.
	if localUserLogin(s) { // 这一步值检查ip和root,并没有检测密码,在外面下一个环节检测
		return nil
	}

	user := s.User()

	// Server salt.
	salt := s.Salt()
	strSalt := common.BytesToString(salt)
	log.Info("gry---server salt: %+v", strSalt)
	// Client response.
	resp := s.Scramble()
	strResp := common.BytesToString(resp)
	log.Info("gry---client resp: %+v", strResp)

	query := fmt.Sprintf("select authentication_string from mysql.user where user='%s'", user)
	log.Info("gry---执行单条query: %+v", query)
	qr, err := spanner.ExecuteSingle(query)

	// Query error.
	if err != nil {
		log.Error("gry---执行单条query err")
		log.Error("proxy: auth.error:%+v", err)
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}

	// User not exists.结果集为0,说明一个user都没
	if len(qr.Rows) == 0 {
		log.Error("proxy: auth.can't.find.the.user:%s", user)
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}

	// mysql.user.authentication_string is ['*' + HEX(SHA1(SHA1(password)))]
	log.Info("gry--qr.Rows[0][0]: %+v", qr.Rows[0][0].String())
	authStr := strings.TrimPrefix(qr.Rows[0][0].String(), "*")
	log.Info("gry---authStr: %+v", authStr)
	wantStage2, err := hex.DecodeString(authStr)
	log.Info("gry---wantStage2: %+v", wantStage2)
	if err != nil {
		log.Error("proxy: auth.user[%s].decode[%s].error:%+v", user, authStr, err)
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}

	// last= SHA1(salt <concat> SHA1(SHA1(password)))
	crypt := sha1.New()
	crypt.Write(salt)
	crypt.Write(wantStage2)
	want := crypt.Sum(nil)
	log.Info("gry---crypt: %+v, want: %+v", crypt, want)

	// gotStage1 = SHA1(password)
	gotStage1 := make([]byte, 20)
	for i := range resp {
		// SHA1(password) = (resp XOR want)
		gotStage1[i] = (resp[i] ^ want[i])
	}
	log.Info("gry---gotStage1: %+v", gotStage1)

	// gotStage2 = SHA1(SHA1(password))
	crypt.Reset()
	crypt.Write(gotStage1)
	gotStage2 := crypt.Sum(nil)
	log.Info("gry---gotStage2: %+v", gotStage2)

	// last= SHA1(salt <concat> SHA1(SHA1(password)))
	crypt.Reset()
	crypt.Write(salt)
	crypt.Write(gotStage2)
	got := crypt.Sum(nil)
	log.Info("gry---crypt: %+v, got: %+v", crypt, got)

	if !bytes.Equal(want, got) {
		log.Warning("spanner.auth\nwant:\n\tstage2:%+v\n\tlast:%+v\ngot\n\tstage2:%+v\n\tlast:%+v\n\n\tsalt:%+v", wantStage2, want, gotStage2, got, salt)
		log.Error("proxy: auth.user[%s].failed(password.invalid):want[%+v]!=got[%+v]", user, want, got)
		return sqldb.NewSQLError(sqldb.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}
	return nil
}
