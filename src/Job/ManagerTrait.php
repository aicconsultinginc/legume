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

use Legume\Job\Worker\ForkWorker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

trait ManagerTrait
{
    /** @var QueueAdaptorInterface $adaptor */
    protected $adaptor;

    /** @var int $last */
    protected $last;

    /** @var LoggerInterface $logger */
    protected $logger;

    /** @var int $size */
    protected $size;

    /** @var ForkWorker[int] */
    protected $workers;

    /**
     * @inheritDoc
     */
    public function __construct(QueueAdaptorInterface $adaptor)
    {
        $this->adaptor = $adaptor;
        $this->logger = new NullLogger();
        $this->running = false;

        $this->workers = array();
        $this->last = 0;
    }

    /**
     * @inheritDoc
     */
    public function submit($task)
    {
        $next = 0;
        if ($this->size > 0) {
            $next = ($this->last + 1) % $this->size;
            if (isset($this->workers[$next])) {
                // Find the worker with less work than our round-robin choice.
                foreach ($this->workers as $i => $worker) {
                    if ($worker->getStacked() < $this->workers[$next]->getStacked()) {
                        $next = $i;
                    }
                }
            }
        }

        if (!isset($this->workers[$next])) {
            // FIXME Replace with $this->createWorker()
            $worker = new ForkWorker();
            $worker->setLogger($this->logger);
            $worker->start();

            // Only add the worker to the pool after start() due to fork.
            $this->workers[$next] = $worker;
        }

        return $this->submitTo($next, $task);
    }

    /**
     * @inheritDoc
     */
    public function submitTo($worker, $task)
    {
        $this->logger->info("Pool submitting task to worker", array($worker));
        if (!isset($this->workers[$worker])) {
            throw new RuntimeException("The selected worker ({$worker}) does not exist!");
        }

        $this->last = $worker;

        return $this->workers[$worker]->stack($task);
    }

    /**
     * @inheritDoc
     */
    public function collect($collector = null)
    {
        if (!isset($collector)) {
            $collector = array($this, "collector");
        }

        $count = 0;
        foreach ($this->workers as $worker) {
            $count += $worker->collect($collector);
        }

        return $count;
    }

    /**
     * @param StackableInterface $task
     *
     * @return bool
     */
    public function collector(StackableInterface $task)
    {
        if ($task->isTerminated()) {
            $this->logger->warning("Job {$task->getId()} failed and will be removed");
            $this->adaptor->retry($task);
        } elseif ($task->isComplete()) {
            $this->logger->info("Job {$task->getId()} completed successfully");
            $this->adaptor->complete($task);
        } else {
            $this->logger->debug("Requesting more time for job {$task->getId()}");
            $this->adaptor->touch($task);
        }

        return $task->isComplete();
    }

    /**
     * @inheritDoc
     */
    public function resize($size)
    {
        $this->size = $size;
    }

    /**
     * @inheritDoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
