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

namespace Legume\Job\Manager;

use Legume\Job\ManagerInterface;
use Legume\Job\QueueAdaptorInterface;
use Legume\Job\StackableInterface;
use Legume\Job\Worker\ForkWorker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class ForkPool implements ManagerInterface
{
    /** @var QueueAdaptorInterface $adaptor */
    protected $adaptor;

    /** @var int $buffer */
    protected $buffer = 5;

    /** @var int $last */
    protected $last;

    /** @var LoggerInterface $logger */
    protected $logger;

    /** @var boolean $running */
    protected $running;

    /** @var int $size */
    protected $size;

    /** @var int $startTime */
    protected $startTime;

    /** @var int $timeout */
    protected $timeout = 1;

    /** @var ForkWorker[int] */
    protected $workers;

    /** @var string $class */
    protected $class;

    /** @var array $ctor */
    protected $ctor;

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

        $this->class = ForkWorker::class;
        $this->ctor = array();
    }

    /**
     * @inheritDoc
     */
    public function shutdown()
    {
        // Cleanup the workers and unstack jobs.
        foreach ($this->workers as $i => $worker) {
            $worker->shutdown();
            // TODO Unstack work
        }

        $this->running = false;
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
            // TODO Type hint interface...
            $worker = new $this->class(...$this->ctor);
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
        if (!is_callable($collector)) {
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
    public function run()
    {
        $this->startTime = time();
        $this->running = true;

        $count = 0;
        $collectionTime = microtime(true);

        while ($this->running) {
            // Check if the pool is at capacity.
            if (($this->size * $this->buffer) > $count) {
                // If the size of the pool is less than the stacked size...
                $task = $this->adaptor->listen($this->timeout);

                if ($task !== null) {
                    // If we received work from the adaptor
                    $this->logger->info("Pool received new job", array($task->getId()));

                    try {
                        $count = $this->submit($task);
                    } catch (RuntimeException $e) {
                        $this->logger->critical($e->getMessage(), $e->getTrace());
                    }
                } elseif (count($this->workers) > 0) {
                    // If there is no more work, clean-up workers.
                    $this->logger->debug("Pool checking worker(s) for idle", array(count($this->workers)));

                    $workers = array();
                    foreach ($this->workers as $i => $worker) {
                        if ($worker->getStacked() < 1) {
                            if (!$worker->isShutdown()) {
                                $this->logger->info("Pool shutting down idle worker", array($i));
                                if (!$worker->shutdown()) {
                                    $this->logger->warning("Pool failed to shut down worker", array($i));
                                }
                                $workers[] = $worker;
                            } else {
                                $this->logger->info("Pool cleaning up worker", array($i));
                                $worker->collect(array($this, "collector"));
                            }
                        } else {
                            $workers[] = $worker;
                        }
                    }
                    $this->workers = $workers;
                }
            } else {
                // Sleep for 500 ms
                sleep($this->timeout);
            }

            if (microtime(true) - $collectionTime >= $this->timeout) {
                $count = $this->collect();
                $collectionTime = microtime(true);
            }
        }

        // Make sure each remaining child is complete...
        foreach ($this->workers as $worker) {
            /** @var ForkWorker $worker */
            if (!$worker->isJoined()) {
                $worker->shutdown();
                $worker->join();
                $worker->collect(array($this, "collector"));
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @inheritDoc
     */
    public function resize($size)
    {
        $this->size = $size;
    }
}
