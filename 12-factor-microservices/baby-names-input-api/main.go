package main

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

var db *sql.DB

type babyInput struct {
	Name string `json:"name"`
	Sex  string `json:"sex"`
}

func main() {
	bindAddr := ":8081"
	connect := "postgres://postgres:password@localhost/babynames?sslmode=disable"

	if v := os.Getenv("BIND_ADDR"); len(v) > 0 {
		bindAddr = v
	}
	if v := os.Getenv("DB_CONNECT"); len(v) > 0 {
		connect = v
	}

	var err error
	db, err = sql.Open("postgres", connect)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/baby", newBaby)

	log.Printf("Listening on %s", bindAddr)
	if err := http.ListenAndServe(bindAddr, nil); err != nil {
		log.Fatal(err)
	}
}

func newBaby(w http.ResponseWriter, req *http.Request) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	var input babyInput
	err = json.Unmarshal(b, &input)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	stmt, err := db.Prepare("INSERT INTO baby_names (name, sex, total) VALUES ($1, $2, 1) ON CONFLICT (name, sex) DO UPDATE SET total = baby_names.total + 1;")
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	if _, err = stmt.Exec(input.Name, input.Sex); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(201)
}
