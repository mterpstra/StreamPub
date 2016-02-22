package main

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	redis "gopkg.in/redis.v3"
)

var redisClient *redis.Client

func uuid() string {
	// generate 32 bits timestamp
	unix32bits := uint32(time.Now().UTC().Unix())

	buff := make([]byte, 12)

	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x-%x", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

func handlePublish(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	documentID := vars["documentid"]

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reply := redisClient.Publish(documentID, string(payload))
	_, err = reply.Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func handlePublishTTL(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	documentID := vars["documentid"]
	ttl := vars["ttl"]

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	uuID := uuid()
	key := fmt.Sprintf("data:%s:%s", documentID, uuID)
	reply := redisClient.Set(key, payload, 0)

	seconds, err := strconv.ParseUint(ttl, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	key = fmt.Sprintf("ttl:%s:%s", documentID, uuID)
	reply = redisClient.Set(key, "", time.Second*time.Duration(seconds))
	_, err = reply.Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	key = fmt.Sprintf("data:%s:%s", documentID, uuID)
	reply = redisClient.Set(key, payload, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {

	var err error

	options := &redis.Options{Network: "tcp", Addr: "localhost:6379"}
	redisClient = redis.NewClient(options)
	defer redisClient.Close()

	r := mux.NewRouter()
	r.HandleFunc("/pub/{documentid}", handlePublish)
	r.HandleFunc("/pub/{documentid}/{ttl}", handlePublishTTL)
	r.Handle("/{rest}", http.FileServer(http.Dir(".")))
	http.Handle("/", r)
	err = http.ListenAndServe(":2000", nil)
	if err != nil {
		panic("Error: " + err.Error())
	}
}
