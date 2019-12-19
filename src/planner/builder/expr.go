/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"fmt"

	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// 中间生成,用于判断
type exprInfo struct {
	// filter expr.
	expr sqlparser.Expr
	// referred tables.
	referTables []string
	// colname in the filter expr.
	cols []*sqlparser.ColName
	// val in the filter expr.
	vals []*sqlparser.SQLVal
}

// parseWhereOrJoinExprs parse exprs in where or join on conditions.
// eg: 't1.a=t2.a and t1.b=2'.
// t1.a=t2.a paser in joins.
// t1.b=2 paser in wheres, t1.b col, 2 val.
// t1.id = t2.id 或者 t1.id > 1 类似,id不管是不是shardkey
// t1Expr 左边返回值, t2Expr 右边返回值
// 这个函数还有一些语义不规范的check
// exprs参数不用加s, 加了引起是list的感觉
func parseWhereOrJoinExprs(exprs sqlparser.Expr, tbInfos map[string]*tableInfo) ([]exprInfo, []exprInfo, error) {
	// 根据and作为分隔符拆分
	// TODO(gry)splitAndExpression-->splitExprsByAnd
	// filters --> exprList
	filters := splitAndExpression(nil, exprs)
	var joins, wheres []exprInfo

	for _, filter := range filters {
		// TODO(gry) col = 1
		var cols []*sqlparser.ColName
		var vals []*sqlparser.SQLVal

		filter = skipParenthesis(filter)
		// convertOrToIn函数里面有判断是不是or expr,不是为什么要传进去呢？为什么不放在外面if or, 然后调用函数
		// b=1 or b=2 转换成in(1,2)
		filter = convertOrToIn(filter)
		// 参数涉及到的表
		referTables := make([]string, 0, 4)
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				cols = append(cols, node)
				tableName := node.Qualifier.Name.String()
				if tableName == "" {
					if len(tbInfos) == 1 {
						// 一个表
						tableName, _ = getOneTableInfo(tbInfos)
					} else {
						return false, errors.Errorf("unsupported: unknown.column.'%s'.in.clause", node.Name.String())
					}
				} else {
					// 检测在不在里面,这里tbInfos
					if _, ok := tbInfos[tableName]; !ok {
						return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.clause", tableName, node.Name.String())
					}
				}

				if isContainKey(referTables, tableName) {
					return true, nil
				}
				referTables = append(referTables, tableName)
			case *sqlparser.Subquery:
				return false, errors.New("unsupported: subqueries.in.select")
			}
			return true, nil
		}, filter)
		if err != nil {
			return nil, nil, err
		}

		// t1.id = t2.id划到jion on
		// t1.id = 2 提取values in(1,2),所以前面有个转换or to in操作
		// 把=值弄出来，condition改名?compareExpr?
		condition, ok := filter.(*sqlparser.ComparisonExpr)
		if ok {
			lc, lok := condition.Left.(*sqlparser.ColName)
			rc, rok := condition.Right.(*sqlparser.ColName)
			switch condition.Operator {
			case sqlparser.EqualStr:
				if lok && rok && lc.Qualifier != rc.Qualifier {
					// TODO(gry) ??
					tuple := exprInfo{condition, referTables, []*sqlparser.ColName{lc, rc}, nil}
					joins = append(joins, tuple)
					continue
				}

				// 走到这一步说明lok或者rok至少有一个为false
				// 这段逻辑可否改成
				// if lok && rok {
				// 	if lc.Qualifier != rc.Qualifier {
				// 	}
				// } else if lok {
				// } else if rok {
				// }

				if lok {
					if sqlVal, ok := condition.Right.(*sqlparser.SQLVal); ok {
						vals = append(vals, sqlVal)
					}
				}
				// 1=a --> a=1
				if rok {
					if sqlVal, ok := condition.Left.(*sqlparser.SQLVal); ok {
						vals = append(vals, sqlVal)
						condition.Left, condition.Right = condition.Right, condition.Left
					}
				}
			case sqlparser.InStr:
				if lok {
					if valTuple, ok := condition.Right.(sqlparser.ValTuple); ok {
						var sqlVals []*sqlparser.SQLVal
						isVal := true
						for _, val := range valTuple {
							if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
								sqlVals = append(sqlVals, sqlVal)
							} else {
								isVal = false
								break
							}
						}
						if isVal {
							vals = sqlVals
						}
					}
				}
				// default ?
			}
		}
		tuple := exprInfo{filter, referTables, cols, vals}
		wheres = append(wheres, tuple)
	}

	return joins, wheres, nil
}

// GetDMLRouting used to get the routing from the where clause.
func GetDMLRouting(database, table, shardkey string, where *sqlparser.Where, router *router.Router) ([]router.Segment, error) {
	if shardkey != "" && where != nil {
		filters := splitAndExpression(nil, where.Expr)
		for _, filter := range filters {
			filter = skipParenthesis(filter)
			filter = convertOrToIn(filter)
			comparison, ok := filter.(*sqlparser.ComparisonExpr)
			if !ok {
				continue
			}

			// Only deal with Equal statement.
			switch comparison.Operator {
			case sqlparser.EqualStr:
				if nameMatch(comparison.Left, table, shardkey) {
					sqlval, ok := comparison.Right.(*sqlparser.SQLVal)
					if ok {
						return router.Lookup(database, table, sqlval, sqlval)
					}
				}
			case sqlparser.InStr:
				if nameMatch(comparison.Left, table, shardkey) {
					if valTuple, ok := comparison.Right.(sqlparser.ValTuple); ok {
						var idxs []int
						for _, val := range valTuple {
							if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
								idx, err := router.GetIndex(database, table, sqlVal)
								if err != nil {
									return nil, err
								}
								idxs = append(idxs, idx)
							} else {
								return router.Lookup(database, table, nil, nil)
							}
						}
						return router.GetSegments(database, table, idxs)
					}
				}
			}
		}
	}
	return router.Lookup(database, table, nil, nil)
}

func nameMatch(node sqlparser.Expr, table, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Qualifier.Name.String() == "" || colname.Qualifier.Name.String() == table) && (colname.Name.String() == shardkey)
}

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	// node 不是filters, 这里node命名应该改为filterExpr,filters-->exprList
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
		// 括号里面and递归, ParenExpr里面也有or，所以这里splitAndExpression函数命名和里面的switch分支注释至少要清晰
	case *sqlparser.ParenExpr:
		if node, ok := node.Expr.(*sqlparser.AndExpr); ok {
			return splitAndExpression(filters, node)
		}
		// default ?
	}
	return append(filters, node)
}

// skipParenthesis skips the parenthesis (if any) of an expression and
// returns the innermost unparenthesized expression.
// 括号去掉, node --- > expr ??
func skipParenthesis(node sqlparser.Expr) sqlparser.Expr {
	if node, ok := node.(*sqlparser.ParenExpr); ok {
		return skipParenthesis(node.Expr)
	}
	return node
}

// splitOrExpression breaks up the OrExpr into OR-separated conditions.
// Split the Equal conditions into inMap, return the other conditions.
// 优化点,一个函数干一个事,优化操作为什么要带进来呢,可以注释说明,或者切分逻辑
// 消除公共子表达式
// (A and B) or (A and C)-->A and ( B or C)
func splitOrExpression(node sqlparser.Expr, inMap map[*sqlparser.ColName][]sqlparser.Expr) []sqlparser.Expr {
	var subExprs []sqlparser.Expr
	switch expr := node.(type) {
	case *sqlparser.OrExpr:
		subExprs = append(subExprs, splitOrExpression(expr.Left, inMap)...)
		subExprs = append(subExprs, splitOrExpression(expr.Right, inMap)...)
	case *sqlparser.ComparisonExpr:
		canSplit := false
		// 等值表达式提取,到后面只用来做路由计算
		// 1=1 ?转换不了,保持原来的状态
		if expr.Operator == sqlparser.EqualStr {
			if lc, ok := expr.Left.(*sqlparser.ColName); ok {
				if val, ok := expr.Right.(*sqlparser.SQLVal); ok {
					// a=b
					col := checkColInMap(inMap, lc)
					inMap[col] = append(inMap[col], val)
					canSplit = true
				}
			} else {
				if rc, ok := expr.Right.(*sqlparser.ColName); ok {
					if val, ok := expr.Left.(*sqlparser.SQLVal); ok {
						col := checkColInMap(inMap, rc)
						inMap[col] = append(inMap[col], val)
						canSplit = true
					}
				}
			}
		}

		// 优化完去掉
		if !canSplit {
			subExprs = append(subExprs, expr)
		}
	case *sqlparser.ParenExpr:
		subExprs = append(subExprs, splitOrExpression(skipParenthesis(expr), inMap)...)
	default:
		subExprs = append(subExprs, expr)
	}
	return subExprs
}

// checkColInMap used to check if the colname is in the map.
// 函数名称:checkColInMap改成类似checkIfNotContainCol?
func checkColInMap(inMap map[*sqlparser.ColName][]sqlparser.Expr, col *sqlparser.ColName) *sqlparser.ColName {
	// 一个if inMap(col) {...} 不就完了嘛?返回值 改为true或者fals,外面调用逻辑改为 if checkIfNotContainCol() { ... }
	// 或者 干脆根本不需要这个check函数,代码直接挪出去判断即可
	for k := range inMap {
		if col.Equal(k) {
			return k
		}
	}
	return col
}

// rebuildOr used to rebuild the OrExpr.
// node改为newExpr, expr改为oldExpr
func rebuildOr(node, expr sqlparser.Expr) sqlparser.Expr {
	if node == nil {
		return expr
	}
	return &sqlparser.OrExpr{
		Left:  node,
		Right: expr,
	}
}

// convertOrToIn used to change the EqualStr to InStr.
func convertOrToIn(node sqlparser.Expr) sqlparser.Expr {
	expr, ok := node.(*sqlparser.OrExpr)
	if !ok {
		return node
	}

	// inMap什么？变量名干嘛的不清晰
	// inMap: in (...)
	inMap := make(map[*sqlparser.ColName][]sqlparser.Expr)
	// result改为newOrExprs
	var result sqlparser.Expr
	// 按顺序,不应该是先or exprs解析出来，然后再convertOrToIn()嘛？现在是convertOrToIn()
	// 里面再做就or exprs的分割,处理顺序可以调整下?
	// subExprs 改为 exprListAfterSplit?
	subExprs := splitOrExpression(expr, inMap)
	for _, subExpr := range subExprs {
		// result 里面的函数叫node, 变量
		result = rebuildOr(result, subExpr)
	}
	for k, v := range inMap {
		subExpr := &sqlparser.ComparisonExpr{
			Operator: sqlparser.InStr,
			Left:     k,
			Right:    sqlparser.ValTuple(v),
		}
		result = rebuildOr(result, subExpr)
	}
	return result
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(joinExpr *sqlparser.JoinTableExpr) {
	// TODO(gry) newExpr -- > leftExpr
	newExpr := joinExpr.LeftExpr
	// If LeftExpr is a join, we have to parenthesize it.
	// 如果left左边是join expr,转换成right tble expr JOIN (expr)
	// e.g.: a join b on .... right join c on ...
	// --->: c left join (a join b on ...) on ...
	if _, ok := newExpr.(*sqlparser.JoinTableExpr); ok {
		newExpr = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newExpr},
		}
	}
	joinExpr.LeftExpr, joinExpr.RightExpr = joinExpr.RightExpr, newExpr
	joinExpr.Join = sqlparser.LeftJoinStr
}

// checkJoinOn use to check the join on conditions, according to lpn|rpn to  determine join.cols[0]|cols[1].
// eg: select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t1.b. 't2.b=t1.b' is forbidden.
func checkJoinOn(lpn, rpn PlanNode, join exprInfo) (exprInfo, error) {
	lt := join.cols[0].Qualifier.Name.String()
	rt := join.cols[1].Qualifier.Name.String()
	if _, ok := lpn.getReferTables()[lt]; ok {
		if _, ok := rpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
	} else {
		if _, ok := lpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
		join.cols[0], join.cols[1] = join.cols[1], join.cols[0]
	}
	return join, nil
}

// parseHaving used to check the having exprs and parse into tuples.
// unsupport: `select sum(t2.id) as tmp, t1.id from t2,t1 having tmp=1`.
func parseHaving(exprs sqlparser.Expr, fields []selectTuple) ([]exprInfo, error) {
	var tuples []exprInfo
	filters := splitAndExpression(nil, exprs)
	for _, filter := range filters {
		filter = skipParenthesis(filter)
		tuple := exprInfo{filter, nil, nil, nil}
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				tuple.cols = append(tuple.cols, node)
				tableName := node.Qualifier.Name.String()
				colName := node.Name.String()
				inField, field := checkInTuple(colName, tableName, fields)
				if !inField {
					col := colName
					if tableName != "" {
						col = fmt.Sprintf("%s.%s", tableName, colName)
					}
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.having.clause", col)
				}
				if field.aggrFuc != "" {
					return false, errors.Errorf("unsupported: aggregation.in.having.clause")
				}

				for _, tb := range field.info.referTables {
					if isContainKey(tuple.referTables, tb) {
						continue
					}
					tuple.referTables = append(tuple.referTables, tb)
				}
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					buf := sqlparser.NewTrackedBuffer(nil)
					node.Format(buf)
					return false, errors.Errorf("unsupported: expr[%s].in.having.clause", buf.String())
				}
			}
			return true, nil
		}, filter)
		if err != nil {
			return nil, err
		}

		tuples = append(tuples, tuple)
	}

	return tuples, nil
}

// getTbsInExpr used to get the referred tables from the expr.
func getTbsInExpr(expr sqlparser.Expr) []string {
	var referTables []string
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if isContainKey(referTables, tableName) {
				return true, nil
			}
			referTables = append(referTables, tableName)
		}
		return true, nil
	}, expr)
	return referTables
}

// replaceCol replace the info.cols based on the colMap.
// eg:
//  select b from (select a+1 as tmp,b from t1)t where tmp > 1;
// If want to replace `tmp>1` with the fields in subquery.
// The `colMap` is built from `a+1 as tmp` and `b`.
// The `info` is built from `tmp>1`.
// We need find and replace the cols from colMap.
// Finally, `tmp > 1` will be overwritten as `a+1 > 1`.
func replaceCol(info exprInfo, colMap map[string]selectTuple) (exprInfo, error) {
	var tables []string
	var columns []*sqlparser.ColName
	for _, old := range info.cols {
		new, err := getMatchedField(old.Name.String(), colMap)
		if err != nil {
			return info, err
		}
		if new.aggrFuc != "" {
			return info, errors.New("unsupported: aggregation.field.in.subquery.is.used.in.clause")
		}

		info.expr = sqlparser.ReplaceExpr(info.expr, old, new.info.expr)
		columns = append(columns, new.info.cols...)
		for _, referTable := range new.info.referTables {
			if !isContainKey(tables, referTable) {
				tables = append(tables, referTable)
			}
		}
	}
	info.cols = columns
	info.referTables = tables
	return info, nil
}
