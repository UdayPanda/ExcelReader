package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuri/excelize/v2"
)

//This function returns table name, first of all asking to user the table name if inputed then executing or if not the taking file name as table
func tableName(path string) string {
	fmt.Println("What will be the name of table : ")
	var name string
	var tableName string
	fmt.Scan(&name)
	if name != "" {
		tableName = name
	} else {
		pathArr := strings.Split(path, "/")
		splitedlem := pathArr[len(pathArr)-1]
		splitelems := strings.Split(splitedlem, ".")
		tableName = splitelems[0]
	}

	return tableName
}

func fetchDatatype(elem string, rows [][]string) string {
	var res string
	match1, _ := regexp.MatchString("^[0-9]*$+", elem)
	match2, _ := regexp.MatchString("[a-zA-Z0-9]+$", elem)
	match3, _ := regexp.MatchString("[A-Za-z]+", elem)

	if match1 {

		if strings.Contains(strings.ToLower(rows[0][0]), "id") {
			res = "INT PRIMARY KEY AUTO_INCREMENT"
		} else {
			res = "INT"
		}

	} else if match2 {
		res = "VARCHAR(50)"
	} else if match3 {
		res = "VARCHAR(50)"
	} else {
		res = "VARCHAR(50)"
	}

	return res
}

func queryCreation(rows [][]string, tableName string) (string, string) {
	var column string
	temp := 0

	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName)
	for i, row := range rows {
		for j, col := range row {
			// fmt.Println(rows[1][j])
			if i == 0 {

				// removing white spaces from cell text (column name)
				if strings.Contains(col, " ") {
					str := strings.Split(col, " ")
					if len(str) >= 3 {
						rmsp := str[0] + str[1] + str[2]
						column += rmsp
					} else if len(str) >= 2 {
						rmsp := str[0] + str[1]
						column += rmsp
					} else {
						column += col
					}
				} else {
					column += col
				}

				if temp < len(row)-1 {
					check := fetchDatatype(rows[1][j], rows)
					// column += " VARCHAR(50), "
					column += fmt.Sprintf(" %s, ", check)
				} else {
					check := fetchDatatype(rows[1][j], rows)
					// column += " VARCHAR(50) "
					column += fmt.Sprintf(" %s ", check)
				}
				temp++
			}
		}
	}
	createTableSQL += column + ");"

	fmt.Println(createTableSQL)

	//validting primary key
	var insertColumns string
	if strings.Contains(strings.ToUpper(createTableSQL), "PRIMARY KEY AUTO_INCREMENT") {
		insertColumns = "("
		for i := 0; i < len(rows[0]); i++ {
			if i == 0 {
				continue
			} else if i == len(rows[0])-1 {
				insertColumns += rows[0][i] + " "
			} else {
				insertColumns += rows[0][i] + ", "
			}
			// fmt.Println(insertColumns)
		}
		insertColumns += ")"
	}

	insertTableSQL := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, insertColumns)
	var elems string
	for i, row := range rows {
		if len(rows[i]) != 0 {
			if i > 0 {
				elems += "("
			}
			for j, col := range row {
				if i >= 1 {
					if strings.Contains(strings.ToLower(rows[0][0]), "id") {
						if j != 0 {
							if j < len(row)-1 {
								elems += fmt.Sprintf("'%s',", strings.TrimSpace(col))
							} else {
								elems += fmt.Sprintf("'%s'", strings.TrimSpace(col))
							}
						}
					} else {
						if j < len(row)-1 {
							elems += fmt.Sprintf("'%s',", strings.TrimSpace(col))
						} else {
							elems += fmt.Sprintf("'%s'", strings.TrimSpace(col))
						}
					}
				}
			}
			if i < len(rows)-1 && i > 0 {
				elems += "),"
			} else if i == 0 {
				continue
			} else {
				elems += ");"
			}
		}
	}

	insertTableSQL += elems

	fmt.Println(insertTableSQL)

	return createTableSQL, insertTableSQL
}

func execution(query, msg string) {
	_, err := conn.Exec(query)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Table %s!\n", msg)
	}
}

var conn *sql.DB

func main() {
	var database string
	fmt.Println("Enter database name : ")
	fmt.Scan(&database)

	credintial := fmt.Sprintf("root:mysql@123@tcp(0.0.0.0:3306)/%s", database)

	//Database Connection

	db, err := sql.Open("mysql", credintial)
	if err != nil {
		log.Fatal(err)
	}
	conn = db
	defer db.Close()

	//Taking file path from user
	var in string
	fmt.Println("Enter path of Excel file : ")
	fmt.Scan(&in)

	f, err := excelize.OpenFile(in)
	if err != nil {
		log.Fatal(err)
	}

	//Taking sheet name from user
	var sheet string
	fmt.Println("Enter sheet name : ")
	fmt.Scan(&sheet)

	rows, err := f.GetRows(sheet)
	if err != nil {
		log.Fatal(err)
	}

	tableName := tableName(in)

	createQuery, insertQuery := queryCreation(rows, tableName)
	execution(createQuery, "updated")
	execution(insertQuery, "updated")
}
