package main

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

type QueryError struct {
	msg string
}

func (d *QueryError) Error() string {
	return d.msg
}
func NewQueryError(m string) *QueryError {
	return &QueryError{msg: m}
}

type QueryResult struct {
	sql     string
	columns []string
	table   [][]dbtype
	message string // message to feed back to user for queries with no output e.g. 'Table created'
}

func NewQueryResult(sql string, columns []string, table [][]dbtype, m string) *QueryResult {
	return &QueryResult{sql: sql, columns: columns, table: table, message: m}
}

func where(whereExpr sqlparser.Expr, table *TableDescription, head Nextable) (Nextable, error) {
	switch whereExpr := whereExpr.(type) {
	case *sqlparser.AndExpr:
		slog.Debug("And condition found", "left", whereExpr.Left, "right", whereExpr.Right)
		step1, _ := where(whereExpr.Left, table, head)
		head = step1
		step2, _ := where(whereExpr.Right, table, head)
		head = step2
		return head, nil
	case *sqlparser.ComparisonExpr:
		left := whereExpr.Left
		var col Column
		var error any
		var p *Page
		var it Iterator
		switch left := left.(type) {
		case *sqlparser.ColName:
			// c = left.Name
			// fmt.Print(c)
			col, error = table.getColumnByName(sqlparser.String(left.Name))
			if error != nil {
				return nil, &QueryError{fmt.Sprintf("Column %v does not exist", sqlparser.String(left.Name))}
			}
			p = bm.getPage(PageID(col.PageIDs[0])) // for now just take the first page

			if head == nil {
				// create iterator
				slog.Debug("Head is empty so create new iterator")
				it = NewIterator(col, &p.Tuples) // This starts a sequencial scan of the rows
				head = &it
				slog.Debug("", "head", head)
			}

		default:
			return nil, &QueryError{"left side not a column name"}
		}
		right := whereExpr.Right
		switch right := right.(type) {
		case *sqlparser.SQLVal:
			// if right.Type == 1 { // this is a int
			//integer
			slog.Debug("Filter with", "filer", sqlparser.String(right), "operand", string(right.Val))
			operand, error := col.strToBytes(string(right.Val))
			if error != nil {
				return nil, &QueryError{"Cannot cast one of the values in a where clause."}
			}
			// binary.BigEndian.PutUint32(operand, uint32(i))
			// slog.Debug("Cast and convert input str (can be int) to bytes", "str", i, "operand", operand)
			filter := NewFilter(whereExpr.Operator, operand, p, head) // cast to a smallint now better would be to change everything to a []byte and implement compare functions based on a type iota
			slog.Debug("Created filter.", "filter", filter)
			head = &filter
			return head, nil
			// }
		default:
			return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", right)}
		}
	}
	return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", whereExpr)}
}

func execute_sql(sql string) (*QueryResult, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// fmt.Printf("Error parsing SQL: %s\n", err.Error())
		return nil, err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// fmt.Printf("SELECT statement: %v\n", sqlparser.String(stmt))
		slog.Debug("", "Columns: ", sqlparser.String(stmt.SelectExprs))
		slog.Debug("", "Table: ", sqlparser.String(stmt.From))

		from := sqlparser.String(stmt.From)
		// fmt.Printf("Table Map %v", tm.TableMap)
		table, error := tm.getTableByName(from)
		if error != nil {
			return nil, &QueryError{fmt.Sprintf("Table %v does not exist.", from)}
		}
		// fmt.Printf("Table object found: %v", table)

		presentationColumns := []Column{}
		for _, c := range stmt.SelectExprs {
			switch c := c.(type) {
			case *sqlparser.AliasedExpr:
				cas := c.Expr
				switch cas := cas.(type) {
				case *sqlparser.ColName:
					col, error := (*table).getColumnByName(sqlparser.String(cas.Name))
					if error != nil {
						return nil, &QueryError{fmt.Sprintf("Column %v does not exist\n", sqlparser.String(cas.Name))}
					}
					presentationColumns = append(presentationColumns, col)
					// fmt.Printf("Alias Expr not implemented.\n")
				default:
					return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt)}
				}
			case *sqlparser.StarExpr:
				presentationColumns = table.Columns
			case *sqlparser.Nextval:
				return nil, &NotImplementedError{"Nextval not implemented."}
			default:
				return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt)}
			}

		}

		// p := bm.getPage(PageID(cols[0].PageIDs[0])) // for now just take the first column from the where statement
		// fmt.Printf("Page ID is %v with tuples: %v", p.PageID, p.Tuples)

		// // create iterator
		// it := NewIterator(&p.Tuples) // This starts a sequencial scan of the rows
		var head Nextable

		if stmt.GroupBy != nil {
			return nil, &NotImplementedError{"Group by not implemented"}
			// for _, n := range stmt.GroupBy {
			// 	fmt.Printf("%v\n", sqlparser.String(n))
			// }
			// fmt.Printf("Group clause: %v\n", sqlparser.String(stmt.GroupBy))
		}

		if stmt.Having != nil {
			slog.Error("Having not implemented")
		}
		// filter := Filter{}
		if stmt.Where != nil {
			slog.Debug("", "Where type", stmt.Where.Type)

			slog.Debug("", "Where clause", sqlparser.String(stmt.Where.Expr))
			whereExpr := stmt.Where.Expr

			head, err = where(whereExpr, table, head)
			if err != nil {
				slog.Error("Error while parsing where statement", "Error", err)
			}

			// switch whereExpr := whereExpr.(type) {
			// case *sqlparser.AndExpr:
			// 	head, err = where(whereExpr, table, head)
			// 	return nil, &NotImplementedError{"AndExpr not implemented"}
			// case *sqlparser.ComparisonExpr:
			// 	// fmt.Printf("%v\n", sqlparser.String(whereExpr.Left))
			// 	// fmt.Printf("%v\n", sqlparser.String(whereExpr.Right))
			// 	left := whereExpr.Left
			// 	// var head Nextable
			// 	var it Iterator
			// 	switch left := left.(type) {
			// 	case *sqlparser.ColName:
			// 		// c = left.Name
			// 		// fmt.Print(c)
			// 		col, error := table.getColumnByName(sqlparser.String(left.Name))
			// 		if error != nil {
			// 			return nil, &QueryError{fmt.Sprintf("Column %v does not exist", sqlparser.String(left.Name))}
			// 		}
			// 		p := bm.getPage(PageID(col.PageIDs[0])) // for now just take the first column from the where statement
			// 		// fmt.Printf("Page ID is %v with tuples: %v", p.PageID, p.Tuples)

			// 		// create iterator
			// 		it = NewIterator(&p.Tuples) // This starts a sequencial scan of the rows
			// 		head = &it
			// 	}
			// 	right := whereExpr.Right
			// 	switch right := right.(type) {
			// 	case *sqlparser.SQLVal:
			// 		if right.Type == 1 { // this is a int
			// 			//integer
			// 			slog.Debug("Filter with", "filer", sqlparser.String(right), "operand", string(right.Val))
			// 			i, _ := strconv.Atoi(string(right.Val))
			// 			p := make([]byte, 8)
			// 			if i < 0 {
			// 				return nil, &NotImplementedError{"Negative numbers not inplemented"}
			// 			}
			// 			binary.BigEndian.AppendUint64(p, uint64(i))
			// 			filter = NewFilter(whereExpr.Operator, p, head) // cast to a smallint now better would be to change everything to a []byte and implement compare functions based on a type iota
			// 			slog.Debug("Created filter.", "filter", filter)
			// 			head = &filter
			// 			// filter.child = &it
			// 		}
			// 	default:
			// 		return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", right)}
			// 	}
			// }
		} else {
			// if there is no WHERE we can use any column to interate. So lets take the 0idx.
			p := bm.getPage(PageID(presentationColumns[0].PageIDs[0])) // for now just take the first column from the where statement
			it := NewIterator(presentationColumns[0], &p.Tuples)       // This starts a sequencial scan of the rows
			head = &it
		}
		if stmt.Distinct != "" {
			d := NewDistinct(head, presentationColumns)
			head = &d
			// return nil, &NotImplementedError{"Distinct not implemented"}
			slog.Debug("distinct", "head", head)
			// fmt.Printf("Group clause: %v\n", sqlparser.String(stmt.GroupBy))
		}

		if stmt.OrderBy != nil {
			slog.Debug("order by in query", "stmt", stmt.OrderBy)
			var dir string
			var columnName string
			for _, j := range stmt.OrderBy {
				columnName = sqlparser.String(j.Expr)
				dir = j.Direction
			}
			c, err := table.getColumnByName(columnName)
			if err != nil {
				slog.Error(err.Error())
				return nil, &QueryError{"Column name not found"}
			}
			ob := NewOrderBy(c, head, dir)
			head = &ob
		}

		if stmt.Limit != nil {
			amount_str := sqlparser.String(stmt.Limit.Rowcount)
			amount_int, err := strconv.Atoi(amount_str)
			if err != nil {
				slog.Error("Unable to read int from amount_str.", "amount", amount_str)
			}
			lim := NewLimit(head, amount_int)
			head = &lim
		}

		slog.Debug("Start iterating results", "head", head)
		ans, eot := head.next()
		slog.Debug("First row", "ans", ans, "eot", eot)
		result := []Tuple{}
		for !eot {
			slog.Debug("Row emerged at presentation layer", "tuple", ans)
			result = append(result, ans)
			ans, eot = head.next()
			slog.Debug("End?", "eot", eot)
		}
		slog.Debug("All rows fetched.", "Rows", result)
		// get all of the to be filtered column first sich that the page can be used a maximum number of times

		// get all the columns the user requested.
		// Do this per column such that every page can be used a maximum number of times before swapping
		ret := [][]dbtype{}
		for _, c := range presentationColumns {
			p := bm.getPage(PageID(c.PageIDs[0]))
			tuples := p.getDBTypesByTuples(result)
			ret = append(ret, tuples)
		}

		// in future presentationColumnNames will overwriten with the names of aliases. This is not implemented yet.
		presentationColumnNames := []string{}
		for _, col := range presentationColumns {
			presentationColumnNames = append(presentationColumnNames, col.ColumnName)
		}

		return NewQueryResult(sql, presentationColumnNames, ret, "fetched rows successfuly"), nil

	case *sqlparser.DDL:
		switch stmt.Action {
		case "create":
			tblName := sqlparser.String(stmt.NewName.Name)
			cols := []Column{}
			tsp := stmt.TableSpec
			for _, c := range tsp.Columns {
				col := Column{ColumnName: sqlparser.String(c.Name), ColumnType: c.Type.Type}
				cols = append(cols, col)
			}
			// fmt.Printf("Table to create has columns: %v", cols)
			t := tm.CreateTable(tblName, cols)
			bm.bufferNewTable(t)
			slog.Debug("Created table.", "table name", t.TableName, "columns", t.Columns)
			return NewQueryResult(sql, nil, nil, "Success"), nil
		default:
			return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt.Action)}
		}

	case *sqlparser.Insert:
		switch stmt.Action {
		case "insert":
			tblName := sqlparser.String(stmt.Table.Name)
			table, error := tm.getTableByName(tblName) // because this is a
			if error != nil {
				return nil, &QueryError{fmt.Sprintf("Table %v does not exist.", tblName)}
			}
			rows := stmt.Rows
			switch rows := rows.(type) {
			case sqlparser.Values:
				if len(table.Columns) != len(rows[0]) {
					// fmt.Printf("Number of columns %v do not match query num cols %v\n", len(table.Columns), len(rows[0]))
					return nil, &QueryError{"Number of columns in insert statement does not match with the number of columns in the table."}
				}
				for _, r := range rows {
					for i, cell := range r {
						switch cell := cell.(type) {
						case *sqlparser.SQLVal:
							// fmt.Printf("Values %v\n", string(cell.Val))
							tm.insertIntoTable(table, i, table.SerialRowID, string(cell.Val), int(cell.Type))
						}

					}
					table.SerialRowID++
				}
			}

			return NewQueryResult(sql, nil, nil, "Success"), nil

			// cols := stmt.Columns // there are none
			// insert into every page based on the order of the
			// fmt.Printf("Inserted in table with name %v, rows %v\n", tblName, rows)
			// fmt.Printf("Table now has %v rows\n", tm.getTableByName(tblName).SerialRowID)
			// fmt.Printf("TM Table now looks like %v\n", tm.getTableByName(tblName))
			// fmt.Printf("Page 0,1,2,3 now looks like %v, %v, %v, %v\n", bm.getPage(0).Tuples, bm.getPage(1).Tuples, bm.getPage(2).Tuples, bm.getPage(3).Tuples)

		default:
			return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt.Action)}
		}
	default:
		return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt)}
	}
	return nil, &NotImplementedError{fmt.Sprintf("Unknow statement type: %T\n", stmt)}
}
