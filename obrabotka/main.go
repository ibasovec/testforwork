package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mu    sync.Mutex
	cache *Cache
)

type Cache struct {
	mu    sync.RWMutex
	items map[string]CacheItem
}

type Product struct {
	Data  string   `bson:"data,omitempty"`
	Extra float64  `bson:"extra,omitempty"`
	MsgID string   `bson:"msg_id,omitempty"`
	Tags  []string `bson:"tags,omitempty"`
}

func productToMap(product Product) map[string]interface{} {
	return map[string]interface{}{
		"data":  product.Data,
		"extra": product.Extra,
		"msgID": product.MsgID,
		"tags":  product.Tags,
	}
}

type CacheItem struct {
	Value      interface{}
	Expiration int64
}

func NewCache() *Cache {
	return &Cache{
		items: make(map[string]CacheItem),
	}
}

func (c *Cache) Set(key string, value interface{}, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiration := time.Now().Add(duration).UnixNano()
	c.items[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found || time.Now().UnixNano() > item.Expiration {
		return nil, false
	}
	return item.Value, true
}

// сравнение
func compareMessages(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if !reflect.DeepEqual(v, m2[k]) {
			return false
		}
	}
	return true
}

func connectToMongoDB() (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать клиента MongoDB: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("не удалось подключиться к MongoDB: %w", err)
	}

	return client, nil
}

func MessageHandler(msg *nats.Msg) {
	var message map[string]interface{}

	// Десериализация JSON
	if err := json.Unmarshal(msg.Data, &message); err != nil {
		log.Printf("\nОшибка при десериализации сообщения: %v", err)
		return
	}

	var msgID string
	// Проверка на наличие поля "msg_id"
	id, exists := message["msg_id"]
	if !exists {
		fmt.Println("\nПолучено сообщение без msg_id: ", message)
		fmt.Println("\nЖдём новое сообщение")
		mu.Lock()
		defer mu.Unlock()
		return
	} else {
		fmt.Printf("\nПолучено сообщение с msg_id: ", message)
	}

	// Преобразуем id в строку, если это возможно
	msgID, ok := id.(string)
	if !ok {
		fmt.Println("\nПолучено сообщение с не строковым msg_id, ждём нового сообщения")
		mu.Lock()
		defer mu.Unlock()
		return
	}

	// Проверка на наличие дубликатов в кэше
	cacheValue, found := cache.Get(msgID)
	if found && compareMessages(message, cacheValue.(map[string]interface{})) {
		fmt.Println("\nНайден дубликат в кэше")

		//подключение к Nats
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			fmt.Println("Ошибка подключения к NATS:", err)
			return
		}
		defer nc.Close()

		// Сообщение
		messageRE := "Дубликат"

		// Публикация сообщения в топик
		err = nc.Publish("updates", []byte(messageRE))
		if err != nil {
			fmt.Println("Ошибка отправки сообщения в NATS:", err)
		} else {
			fmt.Println("Сообщение 'Дубликат' успешно отправлено в NATS.")
		}
	} else {
		fmt.Println("\nНе найден дубликат в кэше")
	}

	// Извлечение элемента из кэша и его вывод
	value, found := cache.Get(msgID)
	if found {
		fmt.Printf("\nГоворит КЭШ msgID: %s, value: %v", msgID, value)
	} else {
		fmt.Printf("\nГоворит КЭШ msgID %s не найден или истек.", msgID)
	}

	// Создаем каналы для синхронизации
	cacheDone := make(chan struct{})
	mongoDone := make(chan struct{})

	// Запуск горутины для записи в кэш
	go func() {
		defer close(cacheDone)
		cache.Set(msgID, message, 30*time.Second)
	}()

	// Запуск горутины для записи в MongoDB
	go func() {
		defer close(mongoDone)

		mu.Lock()
		defer mu.Unlock()

		// Подключение к MongoDB
		client, err := connectToMongoDB()
		if err != nil {
			log.Fatal(err)
			fmt.Printf("\nОшибка в подключении к MongoDB\n")
			return
		}
		fmt.Printf("\nПодключено к MongoDB\n")

		defer func() {
			if err := client.Disconnect(context.TODO()); err != nil {
				log.Fatal(err)
			}
		}()

		// Вставка данных в MongoDB
		collection := client.Database("zad1").Collection("cash")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = collection.InsertOne(ctx, message)
		if err != nil {
			log.Printf("nОшибка при вставке документа в MongoDB: %v", err)
			return
		}

		fmt.Printf("nСообщение с msg_id %s успешно добавлено в MongoDB", msgID)
	}()

	// Ожидание завершения обеих горутин
	<-cacheDone
	<-mongoDone

	// Генерация уникального идентификатора
	uniqueID := uuid.New().String()

	// Объявление и копирование карты, чтобы в message не записывался ["unique_id"]
	myMap := make(map[string]interface{})
	for k, v := range message {
		myMap[k] = v
	}

	myMap["unique_id"] = uniqueID

	fmt.Println("\nСообщение с добавленным уникальным значением:", myMap)
	// Извлечение элемента из кэша и его вывод
	value, found = cache.Get(msgID)
	if found {
		fmt.Printf("\n!!!Говорит КЭШ msgID: %s", msgID)
		fmt.Print(", value: ", value)
	} else {
		fmt.Printf("\n!!!Говорит КЭШ msgID %s не найден или истек.n", msgID)
	}

	//отправка в новый топик
	// Сериализация сообщения в JSON
	messageData2, err := json.Marshal(myMap)
	if err != nil {
		log.Fatalf("Ошибка marshalling: %v", err)
	}
	//подключение к Nats
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		fmt.Println("Ошибка подключения к NATS:", err)
		return
	}
	defer nc.Close()
	// Отправка сообщения в топик 'updates'
	err = nc.Publish("topic2", messageData2)
	if err != nil {
		log.Fatalf("Ошибка публикации: %v", err)
	} else {
		fmt.Println("\nОпубликовано в topic2")
	}

	fmt.Println("\n\nРазделитель сообщений------------------------------\n")

}

func main() {
	// Инициализируем кэш
	cache = NewCache()

	// Подключение к MongoDB
	client, err := connectToMongoDB()
	if err != nil {
		log.Fatal(err)
		fmt.Printf("\nОшибка в подключении к MongoDB\n")
		return
	}
	fmt.Printf("\nПодключено к MongoDB\n")

	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()

	cacheDone2 := make(chan bool) // Канал для синхронизации окончания записи в кэш

	// Запуск горутины для записи продуктов в кэш
	go func() {
		defer close(cacheDone2)
		collection := client.Database("zad1").Collection("cash")

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		cur, err := collection.Find(ctx, bson.M{})
		if err != nil {
			log.Printf("Ошибка при извлечении данных из MongoDB: %sn", err)
			return
		}
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			var product Product
			if err := cur.Decode(&product); err != nil {
				log.Printf("Ошибка при декодировании данных из MongoDB: %sn", err)
				return
			}
			cache.Set(product.MsgID, productToMap(product), 120*time.Second)
		}

		if err := cur.Err(); err != nil {
			log.Printf("Ошибка курсора MongoDB: %sn", err)
		}

		fmt.Println("Продукты успешно загружены в кэш")
	}()

	// Ожидание завершения записи в кэш
	<-cacheDone2

	// Подключаемся к NATS
	natsURL := nats.DefaultURL

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("\nОшибка при подключении к NATS: %v", err)
	}
	fmt.Println("Подключено к NATS!")

	// Подписка на топик 'updates' и использование обработчика сообщений
	if _, err := nc.Subscribe("updates", MessageHandler); err != nil {
		log.Fatalf("\nОшибка при подписке на топик: %v", err)
	}

	select {}
}
