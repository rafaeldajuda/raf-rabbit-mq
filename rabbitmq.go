package rabbitmq

import (
	"errors"

	"github.com/streadway/amqp"
)

func ConnRabb(url string) (connectRabbitMQ *amqp.Connection, channelRabbitMQ *amqp.Channel, err error) {

	// Create a new RabbitMQ connection.
	connectRabbitMQ, err = amqp.Dial(url)
	if err != nil {
		return connectRabbitMQ, channelRabbitMQ, err
	}

	// Opening a channel to the RabbitMQ instance over
	// the connection we have already established.
	channelRabbitMQ, err = connectRabbitMQ.Channel()
	if err != nil {
		return connectRabbitMQ, channelRabbitMQ, err
	}

	return connectRabbitMQ, channelRabbitMQ, nil

}

func (rabbitmq *Rabbitmq) New(url string) error {

	cn, ch, err := ConnRabb(url)
	if err != nil {
		return errors.New("error connecting to RabbitMQ err: " + err.Error())
	}

	rabbitmq.Connection = cn
	rabbitmq.Channel = ch

	return nil
}
