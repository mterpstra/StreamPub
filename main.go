package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var redisConnection redis.Conn

func handleSubscription(w http.ResponseWriter, r *http.Request) {
	println("handleSubscription")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		println(err.Error())
		return
	}

	vars := mux.Vars(r)
	device := vars["device"]
	session := vars["session"]

	println("handleSubscription, listening...")
	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			println(err.Error())
			return
		}

		println("---------------------------------------------------")
		println("redisConnection:", redisConnection)
		println("device:", device)
		println("session:", session)
		println("msgtype:", messageType)
		println("message:", string(p))

		redisConnection.Do("PUBLISH", session, p)
	}
}

func main() {

	var err error
	redisConnection, err = redis.Dial("tcp", "localhost:6379")
	if err != nil {
		println(err.Error())
	}
	defer redisConnection.Close()

	r := mux.NewRouter()
	r.HandleFunc("/pub/{device}/{session}", handleSubscription)
	r.Handle("/{rest}", http.FileServer(http.Dir(".")))
	http.Handle("/", r)
	err = http.ListenAndServe(":2000", nil)
	if err != nil {
		panic("Error: " + err.Error())
	}
}
