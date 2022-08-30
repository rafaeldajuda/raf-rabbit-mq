package rabbitmq

import "github.com/streadway/amqp"

type Rabbitmq struct {
	*amqp.Connection
	*amqp.Channel
}

type Rabbit struct {
	Publish DefaultsQueues
	Consume DefaultsQueues
}

type DefaultQueue struct {
	Queue       string
	Exchange    string
	DelayedTime int
	AutoAck     bool
}

type DefaultsQueues struct {
	Queue   DefaultQueue
	Delayed DefaultQueue
	Dlx     DefaultQueue
}
