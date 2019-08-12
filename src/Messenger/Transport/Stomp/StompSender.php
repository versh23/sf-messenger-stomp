<?php

declare(strict_types=1);

namespace Versh23\Messenger\Transport\Stomp;

use Interop\Queue\Exception;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

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
            $this->connection->send($encodedMessage['body'], $encodedMessage['headers'] ?? []);
        } catch (Exception $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }
}
