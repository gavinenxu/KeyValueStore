package main

import (
	bitcask "bitcask-go"
	"encoding/json"
	"errors"
	"net/http"
	"os"
)

var database *bitcask.DB

func init() {
	configs := bitcask.DefaultConfig
	dir, _ := os.MkdirTemp("", "bitcask_test_http")
	configs.DirPath = dir

	db, err := bitcask.OpenDatabase(configs)
	if err != nil {
		panic(err)
	}
	database = db
}

func main() {

	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/list-keys", HandleListKeys)
	http.HandleFunc("/stats", HandleStats)

	_ = http.ListenAndServe(":8080", nil)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	val, err := database.Get([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(string(val))
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	var res map[string]string
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for k, v := range res {
		if err := database.Put([]byte(k), []byte(v)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode("ok")
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if err := database.Delete([]byte(key)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode("ok")
}

func HandleListKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	var res []string
	keys := database.ListKeys()
	for _, k := range keys {
		res = append(res, string(k))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	stats, err := database.Stats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}
