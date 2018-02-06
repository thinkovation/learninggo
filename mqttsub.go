/*
 * Copyright (c) 2018 Gary Barnett.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Derived from the example code found at github.com/eclipse/paho.mqtt.golang
 *
 */

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

/*
  CONFIGURATION
  Edit the constants below to subscribe to an MQTT channel
  For cheerlights, simply give your client a new name, and pop something into Username

*/

// Broker is the address of the broker you're connecting to - for cheerlights this is "tcp://iot.eclipse.org:1883"
const Broker = "tcp://iot.eclipse.org:1883"

// Topic is the name of the topic you want to subscribe to - for cheerlights this is "cheerlights"
const Topic = "cheerlights"

// ClientID is the ClientID that you plan to use with MQTT - for cheerlights this can be anything
const ClientID = "AName"

// QOS determins the qos setting for the MQTT connection - for cheerlights this is 0
const QOS = 0

// UserName is the username you need to connect to MQTT - for cheerlights this can be anything
const UserName = "Anon"

// UserPWD is the password needed to authenticate the user - for cheerlights this can be blank
const UserPWD = ""

// PublishHandler handles incoming messages for the subscription
//
// You can handle this within the assignment of the handler, but I prefer to
// do my handling in a separate function that is called by the handler
func PublishHandler(topic string, payload string) {
	log.Println(topic, "-", payload)
}

// WaitShutdown simply blocks intil a shutdown signal is received
func WaitShutdown() {
	irqSig := make(chan os.Signal, 1)
	signal.Notify(irqSig, syscall.SIGINT, syscall.SIGTERM)

	//Wait interrupt or shutdown request through /shutdown
	select {
	case sig := <-irqSig:
		log.Printf("Shutdown request (signal: %v)", sig)
	}
	log.Printf("Disconnecting Subscriber...")

}

func main() {

	// Set up MQTT client
	opts := MQTT.NewClientOptions()
	opts.AddBroker(Broker)
	opts.SetClientID(ClientID)
	opts.SetUsername(UserName)
	opts.SetPassword(UserPWD)
	opts.SetCleanSession(false)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		PublishHandler(msg.Topic(), string(msg.Payload()))
	})
	// Connect to MQTT - fail if there's an error
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	// Subscribe to the topic - fail if there's an error
	if token := client.Subscribe(Topic, QOS, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	// This is a blocking function that waits for an interrupt then disconnects the client
	WaitShutdown()

	client.Disconnect(250)
	log.Println("Sample Subscriber Disconnected")
}
