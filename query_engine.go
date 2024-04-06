package main

import (
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func execute_sql(sql string) ([]Tuple, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("Error parsing SQL: %s\n", err.Error())
		return nil, err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// fmt.Printf("SELECT statement: %v\n", sqlparser.String(stmt))
		fmt.Printf("Columns: %v\n", sqlparser.String(stmt.SelectExprs))
		fmt.Printf("Table: %v\n", sqlparser.String(stmt.From))

		from := sqlparser.String(stmt.From)
		table := tm.TableMap[from]

		cols := []Column{}
		for _, c := range stmt.SelectExprs {
			switch c := c.(type) {
			case *sqlparser.AliasedExpr:
				cas := c.Expr
				switch cas := cas.(type) {
				case *sqlparser.ColName:
					fmt.Print(cas)
					c := cas.Name
					fmt.Print(c)
					col, success := table.getColumnByName(sqlparser.String(c)) // hardcoded for now. Cannot find a way to get the name out
					if success != true {
						fmt.Printf("Oh no.. column does not exist.")
					}
					cols = append(cols, col)
					// fmt.Printf("Alias Expr not implemented.\n")
				default:
					fmt.Printf("Unknown statement type: %T\n", stmt)
				}
			case *sqlparser.StarExpr:
				fmt.Printf("Star select not implement.\n")
			case *sqlparser.Nextval:
				fmt.Printf("Next val not implement.\n")
			default:
				fmt.Printf("Unknown statement type: %T\n", stmt)
			}

		}

		p := bm.getPage(PageID(cols[0].PageIDs[0])) // for now just take the first column from the where statement
		fmt.Printf("Page ID is %v with tuples: %v", p.PageID, p.Tuples)

		// create iterator
		it := NewIterator(&p.Tuples)

		if stmt.GroupBy != nil {
			for _, n := range stmt.GroupBy {
				fmt.Printf("%v\n", sqlparser.String(n))
			}
			// fmt.Printf("Group clause: %v\n", sqlparser.String(stmt.GroupBy))
		}
		if stmt.Limit != nil {
			fmt.Printf("Limit clause: %v\n", sqlparser.String(stmt.Limit))
		}
		filter := Filter{}
		if stmt.Where != nil {
			fmt.Printf("%v\n", stmt.Where.Type)

			fmt.Printf("Where clause: %v\n", sqlparser.String(stmt.Where.Expr))
			whereExpr := stmt.Where.Expr
			switch whereExpr := whereExpr.(type) {
			case *sqlparser.AndExpr:
				fmt.Printf("%v\n", sqlparser.String(whereExpr.Left))
				fmt.Printf("%v\n", sqlparser.String(whereExpr.Right))
			case *sqlparser.ComparisonExpr:
				fmt.Printf("%v\n", sqlparser.String(whereExpr.Left))
				fmt.Printf("%v\n", sqlparser.String(whereExpr.Right))
				right := whereExpr.Right
				switch right := right.(type) {
				case *sqlparser.SQLVal:
					if right.Type == 1 { // this is a int
						//integer
						fmt.Printf("Filter for %v with bit array %v\n", sqlparser.String(right), string(right.Val))
						i, _ := strconv.Atoi(string(right.Val))
						filter = NewFilter(whereExpr.Operator, smallint(i), &it) // cast to a smallint now better would be to change everything to a []byte and implement compare functions based on a type iota
						// filter.child = &it
						fmt.Print(filter)
					}
				}
			}
		}
		ans, eot := filter.next()
		var result []Tuple
		for !eot {
			fmt.Printf("Ans: %v\n", ans)
			ans, eot = filter.next()
			result = append(result, ans)
		}
		return result, nil
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
			fmt.Printf("Created table with name %v and columns %v\n", t.TableName, t.Columns)

		}
	case *sqlparser.Insert:
		switch stmt.Action {
		case "insert":
			tblName := sqlparser.String(stmt.Table.Name)
			table := tm.getTableByName(tblName) // because this is a
			rows := stmt.Rows
			switch rows := rows.(type) {
			case sqlparser.Values:
				if len(table.Columns) != len(rows[0]) {
					fmt.Printf("Number of columns %v do not match query num cols %v\n", len(table.Columns), len(rows[0]))
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

			// cols := stmt.Columns // there are none
			// insert into every page based on the order of the
			fmt.Printf("Inserted in table with name %v, rows %v\n", tblName, rows)
			fmt.Printf("Table now has %v rows\n", tm.getTableByName(tblName).SerialRowID)
			fmt.Printf("TM Table now looks like %v\n", tm.getTableByName(tblName))
			fmt.Printf("Page 0,1,2,3 now looks like %v, %v, %v, %v\n", bm.getPage(0).Tuples, bm.getPage(1).Tuples, bm.getPage(2).Tuples, bm.getPage(3).Tuples)

		}
	default:
		fmt.Printf("Unknown statement type: %T\n", stmt)
	}
	return []Tuple{}, nil
}
