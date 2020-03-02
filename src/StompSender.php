<?php

declare(strict_types=1);

namespace Versh23\StompTransport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Versh23\StompTransport\Stamp\CloseConnectionStamp;
use Versh23\StompTransport\Stamp\StompStamp;

class StompSender implements SenderInterface
{
    private $serializer;
    private $connection;

    public function __construct(Connection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    /**
     * Sends the given envelope.
     *
     * The sender can read different stamps for transport configuration,
     * like delivery delay.
     *
     * If applicable, the returned Envelope should contain a TransportMessageIdStamp.
     */
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        try {
            $message = $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? []);
        } catch (\Throwable $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        /**
         * @var CloseConnectionStamp|null
         */
        $stamp = $envelope->last(CloseConnectionStamp::class);

        if ($stamp) {
            switch ($stamp->getMod()) {
                case CloseConnectionStamp::MOD_CONSUMER:
                    $this->connection->closeConsumer();
                    break;
                case CloseConnectionStamp::MOD_PRODUCER:
                    $this->connection->closeProducer();
                    break;
                case CloseConnectionStamp::MOD_ALL:
                    $this->connection->close();
                    break;
            }
        }

        return $envelope->with(new StompStamp($message));
    }
}
