<?php

declare(strict_types=1);

namespace Versh23\Messenger\Transport\Stomp;

use Enqueue\Stomp\StompConnectionFactory;
use Enqueue\Stomp\StompConsumer;
use Enqueue\Stomp\StompContext;
use Enqueue\Stomp\StompDestination;
use Enqueue\Stomp\StompMessage;
use Enqueue\Stomp\StompProducer;

class Connection
{
    private $context;
    private $destinationName;

    private $destination = null;
    private $consumer = null;
    private $producer = null;

    private $receiveTimeout;

    public function __construct(StompContext $context, string $destinationName, int $receiveTimeout)
    {
        $this->context = $context;
        $this->destinationName = $destinationName;
        $this->receiveTimeout = $receiveTimeout;
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

        $factory = new StompConnectionFactory($config);
        $context = $factory->createContext();

        return new self($context, $destinationName, $receiveTimeout);
    }

    public function send(string $body, array $headers = []): StompMessage
    {
        $message = $this->context->createMessage($body, [], $headers);

        $this->getProducer()->send($this->getDestination(), $message);

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

    private function getDestination(): StompDestination
    {
        if (!$this->destination) {
            $this->destination = $this->context->createDestination($this->destinationName);
        }

        return $this->destination;
    }

    private function getConsumer(): StompConsumer
    {
        if (!$this->consumer) {
            $this->consumer = $this->context->createConsumer($this->getDestination());
        }

        return $this->consumer;
    }

    private function getProducer(): StompProducer
    {
        if (!$this->producer) {
            $this->producer = $this->context->createProducer();
        }

        return $this->producer;
    }
}
