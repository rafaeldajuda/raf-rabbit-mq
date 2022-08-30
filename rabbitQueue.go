package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

func (rmq *Rabbitmq) ConfigConsumeQueues(rabbit Rabbit, queue bool, delayed bool, dlx bool) error {

	if queue {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Consume.Queue.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchange %s err: %s", rabbit.Consume.Queue.Exchange, err.Error())
		}

		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Consume.Queue.Queue, // name
			true,                       // durable
			false,                      // delete when unused
			false,                      // exclusive
			false,                      // no-wait
			amqp.Table{"x-dead-letter-exchange": rabbit.Consume.Delayed.Exchange},
		); err != nil {
			return fmt.Errorf("failed to declare a queue %s err: %s", rabbit.Consume.Queue.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Consume.Queue.Queue,
			rabbit.Consume.Queue.Queue,
			rabbit.Consume.Queue.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to exchange %s err: %s", rabbit.Consume.Queue.Queue, rabbit.Consume.Queue.Exchange, err.Error())
		}

	}

	if delayed {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Consume.Delayed.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchangeDelayed %s err: %s", rabbit.Consume.Delayed.Exchange, err.Error())
		}

		// fila que renevia mensagens a cada x minutos (xxxx ms)
		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Consume.Delayed.Queue,
			true,
			false,
			false,
			false,
			amqp.Table{"x-message-ttl": rabbit.Consume.Delayed.DelayedTime, "x-dead-letter-exchange": rabbit.Consume.Queue.Exchange, "x-dead-letter-routing-key": rabbit.Consume.Queue.Queue},
		); err != nil {
			return fmt.Errorf("error declaring queueDelayed %s err: %s", rabbit.Consume.Delayed.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Consume.Delayed.Queue,
			rabbit.Consume.Delayed.Queue,
			rabbit.Consume.Delayed.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to exchange %s err: %s", rabbit.Consume.Delayed.Queue, rabbit.Consume.Delayed.Exchange, err.Error())
		}

	}

	if dlx {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Consume.Dlx.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchangeDlx %s err: %s", rabbit.Consume.Dlx.Exchange, err.Error())
		}

		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Consume.Dlx.Queue,
			true,
			false,
			false,
			false,
			amqp.Table{"x-dead-letter-exchange": rabbit.Consume.Dlx.Exchange, "x-dead-letter-routing-key": rabbit.Consume.Dlx.Queue},
		); err != nil {
			return fmt.Errorf("error declaring queue %s err: %s", rabbit.Consume.Dlx.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Consume.Dlx.Queue,
			rabbit.Consume.Dlx.Queue,
			rabbit.Consume.Dlx.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to exchange %s err: %s", rabbit.Consume.Dlx.Queue, rabbit.Consume.Dlx.Exchange, err.Error())
		}

	}

	return nil
}

func (rmq *Rabbitmq) ConfigPublishQueues(rabbit Rabbit, queue bool, delayed bool, dlx bool) error {

	if queue {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Publish.Queue.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchange (publish) %s err: %s", rabbit.Publish.Queue.Exchange, err.Error())
		}

		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Publish.Queue.Queue, // name
			true,                       // durable
			false,                      // delete when unused
			false,                      // exclusive
			false,                      // no-wait
			amqp.Table{"x-dead-letter-exchange": rabbit.Publish.Delayed.Exchange},
		); err != nil {
			return fmt.Errorf("failed to declare a publish queue %s err: %s", rabbit.Publish.Queue.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Publish.Queue.Queue,
			rabbit.Publish.Queue.Queue,
			rabbit.Publish.Queue.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to publish exchange %s err: %s", rabbit.Publish.Queue.Queue, rabbit.Publish.Queue.Exchange, err.Error())
		}

	}

	if delayed {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Publish.Delayed.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchangeDelayed %s err: %s", rabbit.Publish.Delayed.Exchange, err.Error())
		}

		// fila que renevia mensagens a cada x minutos (xxxx ms)
		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Publish.Delayed.Queue,
			true,
			false,
			false,
			false,
			amqp.Table{"x-message-ttl": rabbit.Publish.Delayed.DelayedTime, "x-dead-letter-exchange": rabbit.Publish.Queue.Exchange, "x-dead-letter-routing-key": rabbit.Publish.Queue.Queue},
		); err != nil {
			return fmt.Errorf("error declaring queueDelayed %s err: %s", rabbit.Publish.Delayed.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Publish.Delayed.Queue,
			rabbit.Publish.Delayed.Queue,
			rabbit.Publish.Delayed.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to exchange %s err: %s", rabbit.Publish.Delayed.Queue, rabbit.Publish.Delayed.Exchange, err.Error())
		}

	}

	if dlx {

		if err := rmq.Channel.ExchangeDeclare(
			rabbit.Publish.Dlx.Exchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error declaring exchangeDlx %s err: %s", rabbit.Publish.Dlx.Exchange, err.Error())
		}

		if _, err := rmq.Channel.QueueDeclare(
			rabbit.Publish.Dlx.Queue,
			true,
			false,
			false,
			false,
			amqp.Table{"x-dead-letter-exchange": rabbit.Publish.Dlx.Exchange, "x-dead-letter-routing-key": rabbit.Publish.Dlx.Queue},
		); err != nil {
			return fmt.Errorf("error declaring queue %s err: %s", rabbit.Publish.Dlx.Queue, err.Error())
		}

		if err := rmq.Channel.QueueBind(
			rabbit.Publish.Dlx.Queue,
			rabbit.Publish.Dlx.Queue,
			rabbit.Publish.Dlx.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error binding queue %s to exchange %s err: %s", rabbit.Publish.Dlx.Queue, rabbit.Publish.Dlx.Exchange, err.Error())
		}

	}

	return nil
}
