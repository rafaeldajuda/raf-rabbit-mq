package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

type TestEnv struct {
	rabbitURL string

	ServOne Rabbit
	ServTwo Rabbit
}

type InFrutas struct {
	Banana  string `json:"banana"`
	Maca    string `json:"maca"`
	Laranja string `json:"laranja"`
}

type OutFrutas struct {
	SucoBanana  string `json:"suco_banana"`
	SucoMaca    string `json:"suco_maca"`
	SucoLaranja string `json:"suco_laranja"`
}

func TestPublish(t *testing.T) {

	// CONFIG ENV
	testEnv := TestEnv{
		rabbitURL: "amqp://guest:guest@0.0.0.0:5672",
		ServOne: Rabbit{
			Publish: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "pqu_30082022",
					Exchange:    "ex_pqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "",
					Exchange:    "exd_pqu_30082022",
					DelayedTime: 0,
				},
				Dlx: DefaultQueue{
					Queue:       "",
					Exchange:    "",
					DelayedTime: 0,
				},
			},
			Consume: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "cqu_30082022",
					Exchange:    "ex_cqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "dl_cqu_30082022",
					Exchange:    "exd_cqu_30082022",
					DelayedTime: 60000,
				},
				Dlx: DefaultQueue{
					Queue:       "dlx_cqu_30082022",
					Exchange:    "ex_dlx_cqu_30082022",
					DelayedTime: 0,
				},
			},
		},
	}

	// START CONNECTION
	rabbitmq := Rabbitmq{}
	err := rabbitmq.New(testEnv.rabbitURL)
	if err != nil {
		log.Fatalf("error connect to rabbitmq: %s", err.Error())
	}

	// CREATE QUEUES - CONSUME/PUBLISH
	rabbitmq.ConfigConsumeQueues(testEnv.ServOne, true, true, true)
	rabbitmq.ConfigPublishQueues(testEnv.ServOne, true, false, false)

	// PUBLISH
	frutas := OutFrutas{
		SucoBanana:  "gosto de goiaba",
		SucoMaca:    "aparencia de mamao",
		SucoLaranja: "que é de limão",
	}
	frutasB, _ := json.Marshal(frutas)

	header := amqp.Table{
		"clientid": "abc",
	}

	time.Sleep(5 * time.Second)
	err = rabbitmq.Publish("teste-rabbit", frutasB, header, testEnv.ServOne.Publish.Queue, "queue")
	if err != nil {
		log.Fatalf("error publish queue: %s", err.Error())
	}

}

func TestPublishDelayedConsume(t *testing.T) {

	// CONFIG ENV
	testEnv := TestEnv{
		rabbitURL: "amqp://guest:guest@0.0.0.0:5672",
		ServOne: Rabbit{
			Publish: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "pqu_30082022",
					Exchange:    "ex_pqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "",
					Exchange:    "exd_pqu_30082022",
					DelayedTime: 0,
				},
				Dlx: DefaultQueue{
					Queue:       "",
					Exchange:    "",
					DelayedTime: 0,
				},
			},
			Consume: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "cqu_30082022",
					Exchange:    "ex_cqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "dl_cqu_30082022",
					Exchange:    "exd_cqu_30082022",
					DelayedTime: 60000,
				},
				Dlx: DefaultQueue{
					Queue:       "dlx_cqu_30082022",
					Exchange:    "ex_dlx_cqu_30082022",
					DelayedTime: 0,
				},
			},
		},
	}

	// START CONNECTION
	rabbitmq := Rabbitmq{}
	err := rabbitmq.New(testEnv.rabbitURL)
	if err != nil {
		log.Fatalf("error connect to rabbitmq: %s", err.Error())
	}

	// CREATE QUEUES - CONSUME/PUBLISH
	rabbitmq.ConfigConsumeQueues(testEnv.ServOne, true, true, true)
	rabbitmq.ConfigPublishQueues(testEnv.ServOne, true, false, false)

	// PUBLISH
	frutas := OutFrutas{
		SucoBanana:  "gosto de goiaba",
		SucoMaca:    "aparencia de mamao",
		SucoLaranja: "que é de limão",
	}
	frutasB, _ := json.Marshal(frutas)

	header := amqp.Table{
		"clientid": "abc",
	}

	// publish delayed
	err = errors.New("func Publish() - erro na publicacao")
	if err != nil {
		fmt.Printf("error publish queue: %s", err.Error())

		// add retry
		header["retry"] = 1

		// publicar na fila de delayed de consumo
		err := rabbitmq.Publish("teste-rabbit", frutasB, header, testEnv.ServOne.Consume.Delayed, "delayed")
		if err != nil {
			log.Fatalf("error publish queue: %s", err.Error())
		}
	}

}

func TestPublishDLXConsume(t *testing.T) {

	// CONFIG ENV
	testEnv := TestEnv{
		rabbitURL: "amqp://guest:guest@0.0.0.0:5672",
		ServOne: Rabbit{
			Publish: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "pqu_30082022",
					Exchange:    "ex_pqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "",
					Exchange:    "exd_pqu_30082022",
					DelayedTime: 0,
				},
				Dlx: DefaultQueue{
					Queue:       "",
					Exchange:    "",
					DelayedTime: 0,
				},
			},
			Consume: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "cqu_30082022",
					Exchange:    "ex_cqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "dl_cqu_30082022",
					Exchange:    "exd_cqu_30082022",
					DelayedTime: 60000,
				},
				Dlx: DefaultQueue{
					Queue:       "dlx_cqu_30082022",
					Exchange:    "ex_dlx_cqu_30082022",
					DelayedTime: 0,
				},
			},
		},
	}

	// START CONNECTION
	rabbitmq := Rabbitmq{}
	err := rabbitmq.New(testEnv.rabbitURL)
	if err != nil {
		log.Fatalf("error connect to rabbitmq: %s", err.Error())
	}

	// CREATE QUEUES - CONSUME/PUBLISH
	rabbitmq.ConfigConsumeQueues(testEnv.ServOne, true, true, true)
	rabbitmq.ConfigPublishQueues(testEnv.ServOne, true, false, false)

	// PUBLISH
	frutas := OutFrutas{
		SucoBanana:  "gosto de goiaba",
		SucoMaca:    "aparencia de mamao",
		SucoLaranja: "que é de limão",
	}
	frutasB, _ := json.Marshal(frutas)

	header := amqp.Table{
		"clientid": "abc",
	}

	// publish dlx
	err = errors.New("func Publish() - erro na publicacao")
	if err != nil {
		fmt.Printf("error publish queue: %s", err.Error())

		// publicar na fila de dlx de consumo
		err := rabbitmq.Publish("teste-rabbit", frutasB, header, testEnv.ServOne.Consume.Dlx, "dlx")
		if err != nil {
			log.Fatalf("error publish queue: %s", err.Error())
		}
	}

}

func TestConsumeQueueSuccess(t *testing.T) {

	// CONFIG ENV
	testEnv := TestEnv{
		rabbitURL: "amqp://guest:guest@0.0.0.0:5672",
		ServOne: Rabbit{
			Publish: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "pqu_30082022",
					Exchange:    "ex_pqu_30082022",
					DelayedTime: 0,
				},
				Delayed: DefaultQueue{
					Queue:       "",
					Exchange:    "exd_pqu_30082022",
					DelayedTime: 0,
				},
				Dlx: DefaultQueue{
					Queue:       "",
					Exchange:    "",
					DelayedTime: 0,
				},
			},
			Consume: DefaultsQueues{
				Queue: DefaultQueue{
					Queue:       "cqu_30082022",
					Exchange:    "ex_cqu_30082022",
					DelayedTime: 0,
					AutoAck:     true,
				},
				Delayed: DefaultQueue{
					Queue:       "dl_cqu_30082022",
					Exchange:    "exd_cqu_30082022",
					DelayedTime: 60000,
				},
				Dlx: DefaultQueue{
					Queue:       "dlx_cqu_30082022",
					Exchange:    "ex_dlx_cqu_30082022",
					DelayedTime: 0,
				},
			},
		},
	}

	// START CONNECTION
	rabbitmq := Rabbitmq{}
	err := rabbitmq.New(testEnv.rabbitURL)
	if err != nil {
		log.Fatalf("error connect to rabbitmq: %s", err.Error())
	}

	// CREATE QUEUES - CONSUME/PUBLISH
	rabbitmq.ConfigConsumeQueues(testEnv.ServOne, true, true, true)
	rabbitmq.ConfigPublishQueues(testEnv.ServOne, true, false, false)

	// CONSUME
	go func() {
		err := rabbitmq.Consume(testEnv.ServOne.Consume, testEnv.ServOne.Publish.Queue, true, 24)
		if err != nil {
			log.Fatal("error creating consumer service:", err.Error())
		}
	}()

	time.Sleep(5 * time.Second)

}
