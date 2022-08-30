package rabbitmq

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func (rabbitmq *Rabbitmq) Publish(messageID string, servInput []byte, header amqp.Table, rabbit DefaultQueue, operation string) (err error) {

	if operation == "queue" {
		delete(header, "retry")
	}

	err = rabbitmq.Channel.Publish(
		rabbit.Exchange, // exchange
		rabbit.Queue,    // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			Headers:     header,
			ContentType: "application/json",
			MessageId:   messageID,
			Timestamp:   time.Now(),
			Body:        servInput,
		}, // message to publish
	)
	if err != nil {
		return errors.New("failed to publish a message: " + err.Error())
	}

	return
}

func (rabbitmq *Rabbitmq) Consume(consume DefaultsQueues, publish DefaultQueue) error {

	// CONSUME QUEUES
	msgs, err := rabbitmq.Channel.Consume(
		consume.Queue.Queue,   // queue
		consume.Queue.Queue,   // consumer
		consume.Queue.AutoAck, // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer %s err: %s", consume.Queue.Queue, err.Error())
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {

			// INPUT					/////////////////////////////////////////////////////////////
			inputBody := d.Body
			inputHeader := d.Headers
			messageID := d.MessageId
			retryAttempt := inputHeader["retry"]

			// PUBLISH MESSAGE			/////////////////////////////////////////////////////////////
			err := rabbitmq.Publish(messageID, inputBody, inputHeader, publish, "queue")
			if err != nil {

				// REPET QUEUE
				if retryAttempt != nil {
					fmt.Printf("[%s] retrying to publish message to exchange: %s", consume.Queue.Queue, consume.Delayed.Queue)
					fmt.Printf("[%s] retrying publish message body: %+v", consume.Queue.Queue, string(inputBody))
					fmt.Printf("retrying attempt: %v", retryAttempt)
				} else {
					fmt.Printf("[%s] publishing message to exchange: %s", consume.Queue.Queue, consume.Delayed.Queue)
					fmt.Printf("[%s] publish message body: %+v", consume.Queue.Queue, string(inputBody))
				}

				var counter int
				if retryAttempt == nil {
					counter = 1
					inputHeader["retry"] = counter
				} else {
					counter, _ = strconv.Atoi(fmt.Sprintf("%v", retryAttempt))
					counter++
					inputHeader["retry"] = counter
				}

				// DEAD QUEUE
				if counter > 24 {
					err = rabbitmq.Publish("teste-rabbit", inputBody, inputHeader, consume.Dlx, "dlx")
					if err != nil {
						fmt.Printf("[%s] out: %v", consume.Queue.Queue, err.Error())
						fmt.Printf("[%s] unable to notify [%s] exchange (dead queue)", consume.Queue.Queue, consume.Dlx.Exchange)
					} else {
						fmt.Printf("[%s] successfully posted to exchange (dead queue) %s", consume.Queue.Queue, consume.Dlx.Exchange)
					}
				} // fim DEAD QUEUE

				// RESEND MESSAGE QUEUE
				if counter <= 24 {
					err = rabbitmq.Publish("teste-rabbit", inputBody, inputHeader, consume.Delayed, "delayed")
					if err != nil {
						fmt.Printf("[%s] out: %v", consume.Queue.Queue, err.Error())
						fmt.Printf("[%s] unable to notify [%s] exchange", consume.Queue.Queue, consume.Delayed.Exchange)
					} else {
						fmt.Printf("[%s] successfully posted to exchange %s", consume.Queue.Queue, consume.Delayed.Exchange)
					}
				} // fim RESEND MESSAGE QUEUE

			}

		}
	}()

	<-forever
	return nil
}
