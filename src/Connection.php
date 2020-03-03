<?php

declare(strict_types=1);

namespace Versh23\StompTransport;

use Enqueue\Stomp\StompConnectionFactory;
use Enqueue\Stomp\StompConsumer;
use Enqueue\Stomp\StompMessage;
use Enqueue\Stomp\StompProducer;

class Connection
{
    private $destinationName;

    private $consumer = null;
    private $producer = null;

    private $receiveTimeout;

    private $connectionFactory;

    private $producerContext = null;
    private $consumerContext = null;

    public function __construct(StompConnectionFactory $connectionFactory, string $destinationName, int $receiveTimeout)
    {
        $this->destinationName = $destinationName;
        $this->receiveTimeout = $receiveTimeout;
        $this->connectionFactory = $connectionFactory;
    }

    public static function create(string $dsn, array $options = []): self
    {
        $destinationName = $options['destination'] ?? null;
        $queueName = $options['queue'] ?? null;
        $topicName = $options['topic'] ?? null;

        $receiveTimeout = ($options['receive_timeout'] ?? 30) * 1000;

        if (!$destinationName && $queueName) {
            $destinationName = '/queue/'.$queueName;
        }

        if (!$destinationName && $topicName) {
            $destinationName = '/topic/'.$topicName;
        }

        if (!$destinationName) {
            $destinationName = '/queue/sf-messenger-queue';
        }

        $config = [
            'dsn' => $dsn,
            'host' => $options['host'] ?? null,
            'port' => $options['port'] ?? null,
            'login' => $options['login'] ?? null,
            'password' => $options['password'] ?? null,
            'vhost' => $options['vhost'] ?? null,
            'buffer_size' => $options['buffer_size'] ?? null,
            'connection_timeout' => $options['connection_timeout'] ?? null,
            'sync' => $options['sync'] ?? null,
            'lazy' => $options['lazy'] ?? null,
            'ssl_on' => $options['ssl_on'] ?? null,
            'write_timeout' => $options['write_timeout'] ?? null,
            'read_timeout' => $options['read_timeout'] ?? null,
            'send_heartbeat' => $options['send_heartbeat'] ?? null,
            'receive_heartbeat' => $options['receive_heartbeat'] ?? null,
        ];

        foreach ($config as $k => $v) {
            if (null === $v) {
                unset($config[$k]);
            }
        }

        return new self(new StompConnectionFactory($config), $destinationName, $receiveTimeout);
    }

    public function send(string $body, array $headers = []): StompMessage
    {
        //INIT
        $this->getProducer();

        $destination = $this->producerContext->createDestination($this->destinationName);
        $message = $this->producerContext->createMessage($body, [], $headers);
        $this->getProducer()->send($destination, $message);

        return $message;
    }

    public function get(): ?StompMessage
    {
        /** @var StompMessage|null $message */
        $message = $this->getConsumer()->receive($this->receiveTimeout);

        if (!$message) {
            return null;
        }

        return $message;
    }

    public function ack(StompMessage $stompMessage): void
    {
        $this->getConsumer()->acknowledge($stompMessage);
    }

    public function reject(StompMessage $stompMessage): void
    {
        $this->getConsumer()->reject($stompMessage);
    }

    private function getConsumer(): StompConsumer
    {
        if (!$this->consumerContext) {
            $this->consumerContext = $this->connectionFactory->createContext();
        }

        if (!$this->consumer) {
            $destination = $this->consumerContext->createDestination($this->destinationName);
            $this->consumer = $this->consumerContext->createConsumer($destination);
        }

        return $this->consumer;
    }

    private function getProducer(): StompProducer
    {
        if (!$this->producerContext) {
            $this->producerContext = $this->connectionFactory->createContext();
        }

        if (!$this->producer) {
            $this->producer = $this->producerContext->createProducer();
        }

        return $this->producer;
    }

    public function close(): void
    {
        $this->closeConsumer();
        $this->closeProducer();
    }

    public function closeProducer(): void
    {
        if ($this->producerContext) {
            $this->producerContext->close();
            $this->producerContext = null;
            $this->producer = null;
        }
    }

    public function closeConsumer(): void
    {
        if ($this->consumerContext) {
            $this->consumerContext->close();
            $this->consumerContext = null;
            $this->consumer = null;
        }
    }
}
