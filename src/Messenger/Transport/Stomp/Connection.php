<?php

declare(strict_types=1);

namespace Versh23\Messenger\Transport\Stomp;

use Enqueue\Stomp\StompConnectionFactory;
use Enqueue\Stomp\StompConsumer;
use Enqueue\Stomp\StompContext;
use Enqueue\Stomp\StompDestination;
use Enqueue\Stomp\StompMessage;
use Interop\Queue\Exception;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\InvalidMessageException;
use Stomp\Exception\ConnectionException;

class Connection
{
    private $context;
    private $destinationName;

    private $destination = null;
    private $consumer = null;

    public function __construct(StompContext $context, string $destinationName)
    {
        $this->context = $context;
        $this->destinationName = $destinationName;
    }

    public static function create(string $dsn, array $options = []): self
    {
        $destinationName = $options['destination'] ?? null;
        $queueName = $options['queue'] ?? null;
        $topicName = $options['topic'] ?? null;

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
        ];

        foreach ($config as $k => $v) {
            if (null === $v) {
                unset($config[$k]);
            }
        }

        $factory = new StompConnectionFactory($config);

        return new self($factory->createContext(), $destinationName);
    }

    /**
     * @throws Exception
     * @throws InvalidDestinationException
     * @throws InvalidMessageException
     */
    public function send(string $body, array $headers = []): StompMessage
    {
        $message = $this->context->createMessage($body, [], $headers);

        $this->context->createProducer()->send($this->getDestination(), $message);

        return $message;
    }

    public function get(): ?StompMessage
    {
        /** @var StompMessage|null $message */
        $message = $this->getConsumer()->receiveNoWait();

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

    /**
     * @throws ConnectionException
     */
    public function ping(): void
    {
        $this->context->getStomp()->getConnection()->sendAlive();
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
}
