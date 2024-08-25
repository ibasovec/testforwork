package main

import (
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	// Параметры подключения к NATS
	natsURL := nats.DefaultURL

	// Подключаемся к NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	// Создание сообщения свободного вида
	message := map[string]interface{}{
		"msg_id": "12345",
		"data":   "Привет, мир!",
		"extra":  42, // Можно добавлять произвольные поля
		"tags":   []string{"example", "test"},
	}

	// Сериализация сообщения в JSON
	messageData, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("Error marshalling message: %v", err)
	}

	// Отправка сообщения в топик 'updates'
	err = nc.Publish("updates", messageData)
	if err != nil {
		log.Fatalf("Error publishing message: %v", err)
	}

	log.Println("Message published!")
	// Убеждаясь, что программа не завершится сразу
	//select {}
}
