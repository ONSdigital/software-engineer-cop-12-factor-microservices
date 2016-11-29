package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"

	_ "github.com/lib/pq"
)

var db *sql.DB

func main() {
	dbConnect := "postgres://postgres:password@localhost/babynames?sslmode=disable"

	if v := os.Getenv("POSTGRES"); len(v) > 0 {
		dbConnect = v
	}

	var err error
	db, err = sql.Open("postgres", dbConnect)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Dropping table baby_names")

	stmt, err := db.Prepare("DROP TABLE IF EXISTS baby_names")
	if err != nil {
		log.Fatal(err)
		return
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Println("Creating table baby_names")

	stmt, err = db.Prepare(`CREATE TABLE baby_names ( name varchar(255), sex varchar(6), total integer, PRIMARY KEY (name, sex) )`)
	if err != nil {
		log.Fatal(err)
		return
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Println("Preparing insert statement")

	stmt, err = db.Prepare(`INSERT INTO baby_names (name, sex, total) VALUES ($1,$2,$3) ON CONFLICT (name, sex) DO UPDATE SET total = baby_names.total + $3;`)
	if err != nil {
		log.Fatal(err)
		return
	}

	var record []string

	log.Println("Inserting boys names")

	boysFile, err := os.Open("data/boys_2015.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer boysFile.Close()

	rdr := csv.NewReader(boysFile)

	for {
		record, err = rdr.Read()
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		_, err = stmt.Exec(record[0], "male", record[1])
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	log.Println("Inserting girls names")

	girlsFile, err := os.Open("data/girls_2015.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer girlsFile.Close()
	rdr = csv.NewReader(girlsFile)

	for {
		record, err = rdr.Read()
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		_, err = stmt.Exec(record[0], "female", record[1])
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	log.Println("Completed")
}
