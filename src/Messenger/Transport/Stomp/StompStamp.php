<?php

declare(strict_types=1);

namespace Versh23\Messenger\Transport\Stomp;

use Enqueue\Stomp\StompMessage;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class StompStamp implements NonSendableStampInterface
{
    private $stompMessage;

    public function __construct(StompMessage $stompMessage)
    {
        $this->stompMessage = $stompMessage;
    }

    public function getStompMessage(): StompMessage
    {
        return $this->stompMessage;
    }
}
