<?php
/* Legume: Multi-thread Job Manager and Daemon
 * Copyright (C) 2017-2018 Alexander Barker.  All Rights Received.
 * https://github.com/kwhat/legume/
 *
 * Legume is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Legume is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

namespace Legume\Job;

use Exception;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Stackable implements StackableInterface
{
    /** @var callable $callable */
    protected $callable;

    /** @var bool $complete */
    protected $complete;

    /** @var int $id */
    protected $id;

    /** @var LoggerInterface $logger */
    protected $logger;

    /** @var string $payload */
    protected $payload;

    /** @var bool $terminated */
    protected $terminated;

    /**
     * @inheritDoc
     */
    public function __construct(callable $callable, $id, $payload)
    {
        $this->callable = $callable;
        $this->complete = false;
        $this->id = $id;
        $this->logger = new NullLogger();
        $this->payload = $payload;
        $this->terminated = false;
    }

    /**
     * @inheritDoc
     */
    public function run()
    {
        try {
            // The dependency injector currently owns the callback, synchronize?
            call_user_func($this->callable, $this->id, $this->payload);
        } catch (Exception $e) {
            $this->logger->error($e->getMessage(), $e->getTrace());
            $this->terminated = true;
        }

        $this->complete = true;
    }

    /**
     * @inheritDoc
     */
    public function getId()
    {
        return "{$this->id}";
    }

    /**
     * @inheritDoc
     */
    public function getPayload()
    {
        return $this->payload;
    }

    /**
     * @inheritDoc
     */
    public function isComplete()
    {
        return $this->complete;
    }

    /**
     * @inheritDoc
     */
    public function isTerminated()
    {
        return $this->terminated;
    }

    /**
     * @inheritDoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
