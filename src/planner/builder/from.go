/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"config"
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// TODO(gry) 注释改成一张表的数据源信息, mergeNode全部是叶子结点,
// 这样子parent命名就不合适,容易引起歧义
// tableInfo represents one table information.
type tableInfo struct {
	// database.
	database string
	// table's name.
	tableName string
	// table's alias.
	alias string
	// table's shard key.
	shardKey string
	// table's shard type.
	shardType string
	// table's config.
	tableConfig *config.TableConfig
	// table expression in select ast 'From'.
	tableExpr *sqlparser.AliasedTableExpr
	// table's route.
	Segments []router.Segment `json:",omitempty"`
	// table's parent node, the type always a MergeNode.
	parent *MergeNode
}

/* scanTableExprs analyzes the 'FROM' clause, build a plannode tree.
 * eg: select t1.a, t3.b from t1 join t2 on t1.a=t2.a join t3 on t1.a=t3.a;
 *             JoinNode
 *               /  \
 *              /    \
 *        JoinNode  MergeNode
 *          /  \
 *         /    \
 *  MergeNode  MergeNode
 *  The leaf node is MergeNode, branch node is JoinNode.
 */
func scanTableExprs(log *xlog.Log, router *router.Router, database string, tableExprs sqlparser.TableExprs) (PlanNode, error) {
	// 以逗号分组，比如 select col... from t1 join t2, t3 join t2 ...
	// t1 join t2就是一个table表达式
	// 一条sql过来不一定只有比如AliasedTableExpr这样一种table表达式
	if len(tableExprs) == 1 {
		return scanTableExpr(log, router, database, tableExprs[0])
	}

	var lpn, rpn PlanNode
	var err error
	// 比如t1Expr, t2Expr, t3Expr...
	// 这里扫t1Expr
	if lpn, err = scanTableExpr(log, router, database, tableExprs[0]); err != nil {
		return nil, err
	}
	// 递归t2Expr, t3Expr...,注释的图讲的是一张表的场景，如何merge node, 另外逗号之间的tbl表达式
	// 满足代数交换律,也可以从右往左的顺序.当然也可以循环处理,但是这样就不好实现了.
	if rpn, err = scanTableExprs(log, router, database, tableExprs[1:]); err != nil {
		return nil, err
	}
	return join(log, lpn, rpn, nil, router)
}

// scanTableExpr produces a plannode subtree by the TableExpr.
func scanTableExpr(log *xlog.Log, router *router.Router, database string, tableExpr sqlparser.TableExpr) (PlanNode, error) {
	var err error
	var p PlanNode
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		p, err = scanAliasedTableExpr(log, router, database, tableExpr)
	case *sqlparser.JoinTableExpr:
		p, err = scanJoinTableExpr(log, router, database, tableExpr)
	case *sqlparser.ParenTableExpr:
		p, err = scanTableExprs(log, router, database, tableExpr.Exprs)
		// If finally p is a MergeNode, the pushed query need keep the parenthese.
		setParenthese(p, true)
		// TODO(gry) 这里是不是要放一个default?按照规范
	}
	return p, err
}

// scanAliasedTableExpr produces the table's tableInfo by the AliasedTableExpr, and build a MergeNode subtree.
// TODO(gry): 这里tableInfo应该改为dataSrc,简单易懂
func scanAliasedTableExpr(log *xlog.Log, r *router.Router, database string, tableExpr *sqlparser.AliasedTableExpr) (PlanNode, error) {
	var err error
	// merge node是叶子结点,可以直接下发,join node不是
	mn := newMergeNode(log, r)
	// expr --- > simpleTblExpr,expr不明确
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		// 如果没有指定db,默认采用当前sesson的db,这里就是改写sql的开始了
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		tn := &tableInfo{
			database: expr.Qualifier.String(),
			Segments: make([]router.Segment, 0, 16),
		}
		// TODO(gry)这里重复了吧,上面不是刚改写过数据库??
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		// TODO(gry) 这里确认下expr的改动是否直接影响tableExpr.Expr,如果是就不需要再赋值
		tableExpr.Expr = expr
		tn.tableName = expr.Name.String()
		tn.tableConfig, err = r.TableConfig(tn.database, tn.tableName)
		if err != nil {
			return nil, err
		}
		// TODO(gry) 搞不明白为什么要重复填写信息呢??如果要shardKey或者shardType,
		// 直接设计两个get...接口不就可以拿到了嘛
		tn.shardKey = tn.tableConfig.ShardKey
		tn.shardType = tn.tableConfig.ShardType
		tn.tableExpr = tableExpr

		switch tn.shardType {
		case "GLOBAL":
			mn.nonGlobalCnt = 0
		case "SINGLE":
			mn.indexes = append(mn.indexes, 0)
			mn.nonGlobalCnt = 1
		case "HASH", "LIST":
			// if a shard table hasn't alias, create one in order to push.
			// 改成别名之后,这样子sql下发就可以用别名代替那些子表了.后面确认下到底好在哪,为什么不直接改
			// 包括其他出现该表的地方可以一次性替换,减少工作量,相当于是一个优化
			// 这个很不规范,这个改动,会直接影响到tn.tableExpr, 因为上面刚刚做了赋值
			if tableExpr.As.String() == "" {
				tableExpr.As = sqlparser.NewTableIdent(tn.tableName)
			}
			mn.nonGlobalCnt = 1
		}

		// 这行代码要移到159行之后,或者167行之后
		tn.parent = mn
		// 这里,已经可以通过tn.tableExpr拿到别名,如果case是hash或者list
		// 另外我给GLOBAL和SINGLE也可以做一个别名这样的操作,只不过别名就是其本身,无用功呗
		// 所以,其实这里也可以设置成一个tn.getAlias()接口,如果需要tableInfo的别名的

		tn.alias = tableExpr.As.String()
		// 这段逻辑,可能考虑到GloBal或者SingLE那边没有赋值别名,那这里可不可以是一个优化点
		if tn.alias != "" {
			// 如果有别名,那就用别名替换
			mn.referTables[tn.alias] = tn
		} else {
			mn.referTables[tn.tableName] = tn
		}
		// 子查询还不支持
	case *sqlparser.Subquery:
		err = errors.New("unsupported: subquery.in.select")
	}
	// TODO(gry) 这里是不是也要一个default?

	// 为后面的buildQuery改写sql做准备
	mn.Sel = &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}
	return mn, err
}

// scanJoinTableExpr produces a PlanNode subtree by the JoinTableExpr.
func scanJoinTableExpr(log *xlog.Log, router *router.Router, database string, joinExpr *sqlparser.JoinTableExpr) (PlanNode, error) {
	switch joinExpr.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(joinExpr)
	default:
		return nil, errors.Errorf("unsupported: join.type:%s", joinExpr.Join)
	}
	lpn, err := scanTableExpr(log, router, database, joinExpr.LeftExpr)
	if err != nil {
		return nil, err
	}

	rpn, err := scanTableExpr(log, router, database, joinExpr.RightExpr)
	if err != nil {
		return nil, err
	}
	// TODO(gry) lpn-->leftNodeExpr, rpn-->rightNodeExpr
	return join(log, lpn, rpn, joinExpr, router)
}

// join build a PlanNode subtree by judging whether left and right can be merged.
// If can be merged, left and right merge into one MergeNode.
// else build a JoinNode, the two nodes become new joinnode's Left and Right.
// TODO(gry) scanJoinTableExpr()和scanTableExprs()分别调用,两者处理的逻辑不一致的地方:joinExpr是否为nil
// 可以考虑拆解join,例如:
// jionByPlanNode() {joinImpl()}
// jionByJoinExpr() {joinImpl()}
func join(log *xlog.Log, lpn, rpn PlanNode, joinExpr *sqlparser.JoinTableExpr, router *router.Router) (PlanNode, error) {
	var joinOn, otherJoinOn []exprInfo
	var err error

	// 到208合并到一个函数,判断合并,从叶子节点mergeNode.getReferTables()合并,开始递归
	// TODO(gry)lpn拿到referTables可以直接拿来初始化吧?
	referTables := make(map[string]*tableInfo)
	for k, v := range lpn.getReferTables() {
		referTables[k] = v
	}
	for k, v := range rpn.getReferTables() {
		if _, ok := referTables[k]; ok {
			return nil, errors.Errorf("unsupported: not.unique.table.or.alias:'%s'", k)
		}
		referTables[k] = v
	}
	// 这里只处理on
	// 如果是... from t1, t2...,joinExpr = nil,结构就不一样
	// join -- > 笛卡尔积, 投影---> select listExpr
	if joinExpr != nil {
		if joinExpr.On == nil {
			joinExpr = nil
		} else {
			// 类似 或者a join b on ...
			// Where / Join expr 过滤处理
			if joinOn, otherJoinOn, err = parseWhereOrJoinExprs(joinExpr.On, referTables); err != nil {
				return nil, err
			}
			for i, jt := range joinOn {
				if jt, err = checkJoinOn(lpn, rpn, jt); err != nil {
					return nil, err
				}
				joinOn[i] = jt
			}

			// inner join's other join on would add to where.
			if joinExpr.Join != sqlparser.LeftJoinStr && len(otherJoinOn) > 0 {
				if len(joinOn) == 0 {
					joinExpr = nil
				}
				for idx, join := range joinOn {
					if idx == 0 {
						joinExpr.On = join.expr
						continue
					}
					joinExpr.On = &sqlparser.AndExpr{
						Left:  joinExpr.On,
						Right: join.expr,
					}
				}
			}
		}
	}

	// analyse if can be merged.
	if lmn, ok := lpn.(*MergeNode); ok {
		if rmn, ok := rpn.(*MergeNode); ok {
			// if all of left's or right's tables are global tables.
			if lmn.nonGlobalCnt == 0 || rmn.nonGlobalCnt == 0 {
				return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
			}
			// if join on condition's cols are both shardkey, and the tables have same shards.
			for _, jt := range joinOn {
				if isSameShard(lmn.referTables, rmn.referTables, jt.cols[0], jt.cols[1]) {
					return mergeRoutes(lmn, rmn, joinExpr, otherJoinOn)
				}
			}
		}
	}
	jn := newJoinNode(log, lpn, rpn, router, joinExpr, joinOn, referTables)
	lpn.setParent(jn)
	rpn.setParent(jn)
	if jn.IsLeftJoin {
		jn.setOtherJoin(otherJoinOn)
	} else {
		for _, filter := range otherJoinOn {
			if err := jn.pushFilter(filter); err != nil {
				return jn, err
			}
		}
	}
	return jn, err
}

// mergeRoutes merges two MergeNode to the lmn.
func mergeRoutes(lmn, rmn *MergeNode, joinExpr *sqlparser.JoinTableExpr, otherJoinOn []exprInfo) (*MergeNode, error) {
	var err error
	lSel := lmn.Sel.(*sqlparser.Select)
	rSel := rmn.Sel.(*sqlparser.Select)
	if lmn.hasParen {
		lSel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: lSel.From}}
	}
	if rmn.hasParen {
		rSel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: rSel.From}}
	}
	if joinExpr == nil {
		// select ... from t1, t2
		lSel.From = append(lSel.From, rSel.From...)
	} else {
		// t1 join t2
		lSel.From = sqlparser.TableExprs{joinExpr}
	}

	for k, v := range rmn.getReferTables() {
		v.parent = lmn
		lmn.referTables[k] = v
	}
	if rSel.Where != nil {
		lSel.AddWhere(rSel.Where.Expr)
	}

	lmn.nonGlobalCnt += rmn.nonGlobalCnt
	if joinExpr == nil || joinExpr.Join != sqlparser.LeftJoinStr {
		for _, filter := range otherJoinOn {
			if err := lmn.pushFilter(filter); err != nil {
				return lmn, err
			}
		}
	}
	return lmn, err
}

// isSameShard used to judge lcn|rcn contain shardkey and have same shards.
func isSameShard(ltb, rtb map[string]*tableInfo, lcn, rcn *sqlparser.ColName) bool {
	lt := ltb[lcn.Qualifier.Name.String()]
	if lt.shardKey == "" || lt.shardKey != lcn.Name.String() {
		return false
	}
	ltp := lt.tableConfig.Partitions
	rt := rtb[rcn.Qualifier.Name.String()]
	if rt.shardKey == "" || rt.shardKey != rcn.Name.String() {
		return false
	}
	rtp := rt.tableConfig.Partitions

	if len(ltp) != len(rtp) {
		return false
	}
	for i, lpart := range ltp {
		if lpart.Segment != rtp[i].Segment || lpart.Backend != rtp[i].Backend {
			return false
		}
	}
	return true
}
