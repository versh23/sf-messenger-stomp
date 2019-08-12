<?php

declare(strict_types=1);

namespace Versh23\Messenger\Transport\Stomp;

use Enqueue\Stomp\StompMessage;
use Stomp\Exception\ConnectionException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Exception\LogicException;

class StompReceiver implements ReceiverInterface
{
    private $serializer;
    private $connection;

    public function __construct(Connection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    /**
     * Receives some messages.
     *
     * While this method could return an unlimited number of messages,
     * the intention is that it returns only one, or a "small number"
     * of messages each time. This gives the user more flexibility:
     * they can finish processing the one (or "small number") of messages
     * from this receiver and move on to check other receivers for messages.
     * If this method returns too many messages, it could cause a
     * blocking effect where handling the messages received from one
     * call to get() takes a long time, blocking other receivers from
     * being called.
     *
     * If applicable, the Envelope should contain a TransportMessageIdStamp.
     *
     * If a received message cannot be decoded, the message should not
     * be retried again (e.g. if there's a queue, it should be removed)
     * and a MessageDecodingFailedException should be thrown.
     *
     * @return Envelope[]
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function get(): iterable
    {
        try {
            $this->connection->ping();
        } catch (ConnectionException $e) {
            throw new TransportException($e->getMessage());
        }

        $stompMessage = $this->connection->get();

        if (!$stompMessage) {
            return [];
        }

        try {
            $envelope = $this->serializer->decode([
                'body' => $stompMessage->getBody(),
                'headers' => $stompMessage->getHeaders(),
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $this->rejectStompMessage($stompMessage);

            throw $exception;
        }

        return [$envelope->with(new StompStamp($stompMessage))];
    }

    /**
     * Acknowledges that the passed message was handled.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function ack(Envelope $envelope): void
    {
        try {
            $stamp = $this->findStompStamp($envelope);
            $this->connection->ack($stamp->getStompMessage());
        } catch (\Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * Called when handling the message failed and it should not be retried.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function reject(Envelope $envelope): void
    {
        try {
            $stamp = $this->findStompStamp($envelope);
            $this->connection->reject($stamp->getStompMessage());
        } catch (\Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function findStompStamp(Envelope $envelope): StompStamp
    {
        /** @var StompStamp|null $stompReceivedStamp */
        $stompReceivedStamp = $envelope->last(StompStamp::class);
        if (null === $stompReceivedStamp) {
            throw new LogicException('No "StompReceivedStamp" stamp found on the Envelope.');
        }

        return $stompReceivedStamp;
    }

    private function rejectStompMessage(StompMessage $stompMessage): void
    {
        try {
            $this->connection->reject($stompMessage);
        } catch (\Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }
}
