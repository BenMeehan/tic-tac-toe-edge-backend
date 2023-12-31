package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

const (
	mqttBrokerURL = "tls://s341caa4.ala.us-east-1.emqxsl.com:8883"
	mqttClientID  = "game_processor"
	mqttTopic     = "play_game"
)

var kafkaTopic = "tic-tac-toe"

var kafkaProducer *kafka.Producer

func onMQTTMessageReceived(client MQTT.Client, msg MQTT.Message) {
	id := string(msg.Payload())
	fmt.Printf("Received ID: %s\n", id)

	deliveryChan := make(chan kafka.Event)

	expirationTime := time.Now().Add(1 * time.Minute)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte(id),
		Headers:        []kafka.Header{{Key: "expiration-time", Value: []byte(fmt.Sprintf("%d", expirationTime.UnixNano()/int64(time.Millisecond)))}},
	}
	kafkaProducer.Produce(message, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}

func main() {
	opts := MQTT.NewClientOptions().AddBroker(mqttBrokerURL).SetClientID(mqttClientID).SetUsername("ben").SetPassword("bm12")
	client := MQTT.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
		os.Exit(1)
	}

	if token := client.Subscribe(mqttTopic, 0, onMQTTMessageReceived); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to subscribe to MQTT topic: %v", token.Error())
		os.Exit(1)
	}

	fmt.Println("Listening for messages on MQTT topic 'play_game'...")

	viper.SetConfigName("client")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading client.properties: %v", err)
		os.Exit(1)
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  viper.GetString("bootstrap.servers"),
		"security.protocol":  viper.GetString("security.protocol"),
		"sasl.mechanisms":    viper.GetString("sasl.mechanisms"),
		"sasl.username":      viper.GetString("sasl.username"),
		"sasl.password":      viper.GetString("sasl.password"),
		"enable.idempotence": true,
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
		os.Exit(1)
	}
	kafkaProducer = p

	defer kafkaProducer.Close()

	select {}
}
