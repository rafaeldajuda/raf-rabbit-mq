package rabbitmq

import (
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
		return fmt.Errorf("failed to publish a message: %s", err.Error())
	}

	return
}

func (rabbitmq *Rabbitmq) Consume(consume DefaultsQueues, publish DefaultQueue, retry bool, max int) error {

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

	var forever chan error

	go func() error {
		for d := range msgs {

			// INPUT					/////////////////////////////////////////////////////////////
			inputBody := d.Body
			inputHeader := d.Headers
			messageID := d.MessageId
			retryAttempt := inputHeader["retry"]

			// PUBLISH MESSAGE			/////////////////////////////////////////////////////////////
			err := rabbitmq.Publish(messageID, inputBody, inputHeader, publish, "queue")
			if err != nil {

				// if false, repeat functions enabled
				if retry {
					return fmt.Errorf("unable to notify [%s] exchange (publish queue) err: %s", publish.Exchange, err.Error())
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
				if counter > max {
					err = rabbitmq.Publish(messageID, inputBody, inputHeader, consume.Dlx, "dlx")
					if err != nil {
						return fmt.Errorf("unable to notify [%s] exchange (dead queue) err: %s", consume.Dlx.Exchange, err.Error())
					}
				}

				// RESEND MESSAGE QUEUE
				if counter <= max {
					err = rabbitmq.Publish(messageID, inputBody, inputHeader, consume.Delayed, "delayed")
					if err != nil {
						return fmt.Errorf("unable to notify [%s] exchange (delayed queue) err: %s", consume.Delayed.Exchange, err.Error())
					}
				}

			}

		}

		return nil
	}()

	err = <-forever
	return err
}
