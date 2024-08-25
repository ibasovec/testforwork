package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func main() {

	// Подключение к серверу NATS по стандартному URL
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Println("Ошибка подключения к NATS:", err)
		return
	} else {
		fmt.Println("Подключено")
	}
	defer nc.Close()

	// Подписка на канал "updates"
	_, err = nc.Subscribe("updates", func(m *nats.Msg) {
		fmt.Printf("\nПолучено сообщение: %s", string(m.Data))
		fmt.Println("\n")
	})
	if err != nil {
		fmt.Println("Ошибка подписки на канал 'updates':", err)
		return
	}
	fmt.Println("\n---------------------------------\n")
	// Убеждаясь, что программа не завершится сразу
	select {}
}
