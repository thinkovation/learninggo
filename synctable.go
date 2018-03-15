package main
/*
Synctable synchronises two tables - it receives a source db connection, a target db connection, a table name, and the name of an update 
timestamp column. It then copies records from the source into the target that were updated after the latest update timestamp in the target
*/
import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)
// Copytable copies a table from the fromdb to the todb
// It checks to see whether the table exists in the fromdb
// It checks to see whether the table exists in the todb
// If the table doesn't exist in the todb, it creates the table and loads it with the contents of the table in the fromdb
// If the table does exist then we compare the table specs by looking at the create table statement
// if they are the same then we update the table, if they are not the same
// we rename the old table and create a new one.
func Copytable(fromdb *sql.DB, todb *sql.DB, tablename string, updatetscolname string) string {
	var (
		container []sql.NullString
		pointers  []interface{}
	)
	fromdbcreatetablespec := showtable(fromdb, tablename)
	if fromdbcreatetablespec == "Table Not Found" {
		return "ERROR : Could not find source table"
	}
	todbcreatetablespec := showtable(todb, tablename)
	if strings.HasPrefix(todbcreatetablespec, "ERROR : Table Not Found") {
		fmt.Println("To table not found so we will have to create it")
		execres := execDDL(todb, fromdbcreatetablespec)
		if execres != "OK" {
			return "ERRROR : Could not create table"
		}
	}

	fcol := gettablefields(fromdb, tablename)
	latestts := getLatestUpdateTS(targetdb, tablename, updatetscolname)
	sqlinsert := "REPLACE INTO " + tablename + " ("
	
	for _, v := range fcol {
		sqlinsert = sqlinsert + "`" + v.Field + "`,"
	}
	sqlinsert = sqlinsert[:len(sqlinsert)-1] + ") VALUES "
	sqlselect := "SELECT "
	for _, v := range fcol {
		sqlselect = sqlselect + "`" + v.Field + "`,"
	}

	// This is where we select the rows to be inserted
	// I am going to collect all of the rows and batch them for insert then
	// I will process the inserts
	//
	// I want to be in and out of the production db asap
	sqlselect = sqlselect[:len(sqlselect)-1] + " FROM " + tablename + " WHERE `" + updatetscolname + "` >=	 '" + latestts + "'"
	//fmt.Println("Executing select - ", sqlselect)

	rows, queryerr := sourcedb.Query(sqlselect) // Note: Ignoring errors for brevity
	if queryerr != nil {
		fmt.Println(sqlselect, "-", queryerr)
		return queryerr.Error()
	}

	length := len(fcol)
	var insertvalues []string
	for rows.Next() {

		pointers = make([]interface{}, length)
		container = make([]sql.NullString, length)

		for i := range pointers {
			pointers[i] = &container[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			panic(err.Error())
		}
		tstring := ""
		for k, v := range container {

			//fmt.Print(fcol[k].Field, " - ")
			ftype := ""
			if strings.HasPrefix(fcol[k].FieldType, "double") {
				ftype = "double"
			}
			if strings.HasPrefix(fcol[k].FieldType, "text") {
				ftype = "varchar"
			}
			if strings.HasPrefix(fcol[k].FieldType, "int") {
				ftype = "double"
			}
			if strings.HasPrefix(fcol[k].FieldType, "tiny") {
				ftype = "double"
			}
			if strings.HasPrefix(fcol[k].FieldType, "varchar") {
				ftype = "varchar"
			}
			if strings.HasPrefix(fcol[k].FieldType, "date") {
				ftype = "varchar"
			}
			if strings.HasPrefix(fcol[k].FieldType, "time") {
				ftype = "varchar"
			}
			switch ftype {
			case "double":
				if v.Valid {
					//
					//fmt.Println(v.String)
					tstring = tstring + v.String
				} else {
					//	fmt.Println("NULL")
					tstring = tstring + "NULL"
				}
				tstring = tstring + ","
			case "varchar":
				if v.Valid {
					//	fmt.Println("'" + v.String + "'")
					// Clean string
					tstring = tstring + "'" + strings.Replace(v.String, "'", `\'`, -1) + "'"
				} else {
					//	fmt.Println("NULL")
					tstring = tstring + "NULL"
				}
				tstring = tstring + ","
			default:
				fmt.Println("Unhandled", fcol[k], "- ", v)

			}

		}
	
		tstring = tstring[:len(tstring)-1]
		tstring = "(" + tstring + ")"
		insertvalues = append(insertvalues, tstring)
	}
	fmt.Print(tablename, " - ", len(insertvalues), " rows to copy. ")
	batchcounter := 0
	batchstring := ""
	res := ""
	var reccounter float64
	reccounter = 0
	now := time.Now().UTC()
	var buffer bytes.Buffer

	for _, v := range insertvalues {
		reccounter++
		batchcounter++
		buffer.WriteString(v + ",")
		// The buffer length must be smalled than the max packet size of the db
		if buffer.Len() > 4000000 {
			batchstring = buffer.String()
			batchstring = batchstring[:len(batchstring)-1]
			res = "OK"
			res = execDDL(todb, sqlinsert+batchstring)
			if res != "OK" {
				res = res + " with query " + sqlinsert + batchstring
			}
			fmt.Print(" . ", reccounter, " Records posted ", res)
			batchstring = ""
			buffer.Reset()
			batchcounter = 0
		}

	}
	if batchcounter > 0 {
		batchstring = buffer.String()
		batchstring = batchstring[:len(batchstring)-1]
		res = "OK"
		res = execDDL(todb, sqlinsert+batchstring)
		fmt.Print(" . ", reccounter, " Records posted ", res)

	} else {
		fmt.Println("No records to copy")
	}
	fmt.Println(" Elapsed =", time.Since(now))
	return "OK"
}

func showtablecols(idb *sql.DB, tblname string) {
	cmd := "select * from " + tblname
	rows, dberr := idb.Query(cmd)
	if dberr != nil {
		//return dberr
		fmt.Println(dberr)
	}
	defer rows.Close()
	cols, dberr := rows.Columns()
	fmt.Println(cols)
}

type TblFields struct {
	Field     string
	FieldType string
	Key       string
}

func gettablefields(qdb *sql.DB, tblname string) []TblFields {
	var tmpflds []TblFields
	var tmpfld TblFields
	var blank1, blank2, blank3 sql.NullString
	cmd := "SHOW COLUMNS FROM " + tblname
	rows, _ := qdb.Query(cmd)
	for rows.Next() {
		err := rows.Scan(&tmpfld.Field, &tmpfld.FieldType, &blank1, &tmpfld.Key, &blank2, &blank3)
		if err != nil {
			fmt.Println(err)
		}
		tmpflds = append(tmpflds, tmpfld)
	}
	return tmpflds

}
func getLatestUpdateTS(db *sql.DB, tblname string, tsfield string) string {
	var ts sql.NullString
	cmd := "SELECT max(`" + tsfield + "`) from " + tblname
	rows, dberr := db.Query(cmd)
	if dberr != nil {
		//return dberr

		if strings.HasPrefix(dberr.Error(), "Error 1146") {

			return "ERROR : Table Not Found " + tblname
		}
		return dberr.Error()

	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&ts)
		if err != nil {
			fmt.Println(err)
		}
	}
	if ts.Valid {
		return ts.String
	}
	return "0000-01-01 00:00:00"

}
func showtable(qdb *sql.DB, tblname string) string {
	var table, createtble string
	cmd := "SHOW CREATE TABLE " + tblname
	rows, dberr := qdb.Query(cmd)
	if dberr != nil {
		//return dberr

		if strings.HasPrefix(dberr.Error(), "Error 1146") {

			return "ERROR : Table Not Found " + tblname
		}
		return dberr.Error()

	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&table, &createtble)
		if err != nil {
			fmt.Println(err)
		}
	}
	return createtble
}

// execDDL simply calls a SQL exec query - returning a string containing the result
func execDDL(targetdb *sql.DB, ddl string) string {
	_, err := targetdb.Exec(ddl)
	if err != nil {
		return err.Error()

	}
	return "OK"
}

