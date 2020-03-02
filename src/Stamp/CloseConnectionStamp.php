<?php

declare(strict_types=1);

namespace Versh23\StompTransport\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class CloseConnectionStamp implements NonSendableStampInterface
{
    public const MOD_ALL = 'all';
    public const MOD_PRODUCER = 'producer';
    public const MOD_CONSUMER = 'consumer';

    private $mod;

    public function __construct(string $mod)
    {
        $this->mod = $mod;
    }

    public function getMod(): string
    {
        return $this->mod;
    }
}
