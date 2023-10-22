import paho.mqtt.client as mqtt
import time

# MQTT Broker Settings
mqttBrokerURL = "s341caa4.ala.us-east-1.emqxsl.com"
mqttPort = 8883
mqttTopic = "play_game"

# MQTT Credentials
mqttUsername = "ben"
mqttPassword = "bm12"

clientID = "a"

# Callback when the client connects to the broker


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe("/"+clientID)
        client.publish(mqttTopic, clientID)
    else:
        print(f"Failed to connect to MQTT Broker with code {rc}")

# Callback when a message is published to the subscribed topic


def on_message(client, userdata, message):
    print(
        f"Received message '{message.payload.decode()}' on topic '{message.topic}'")


# Create an MQTT client
client = mqtt.Client(clientID)
client.username_pw_set(username=mqttUsername, password=mqttPassword)

# Set the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.tls_set()
client.connect(mqttBrokerURL, mqttPort, keepalive=60)

# Start the MQTT client loop
client.loop_start()

try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    print("Disconnecting from MQTT Broker")
    client.disconnect()
    client.loop_stop()
