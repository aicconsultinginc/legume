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
use Legume\Job\Stackable\ForkStackable;
use Legume\Job\Worker\ForkWorker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class ForkPool implements ManagerInterface
{
    /** @var QueueAdaptorInterface $adaptor */
    protected $adaptor;

    /** @var LoggerInterface $logger */
    protected $logger;

    /** @var boolean $running */
    protected $running;

    /** @var int $size */
    protected $size;

    /** @var int $startTime */
    protected $startTime;

    /** @var ForkWorker[int] */
    protected $workers;

    /** @var int $last */
    protected $last;

    /** @var int $buffer */
    protected $buffer = 1;

    /**
     * @param QueueAdaptorInterface $adaptor
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
     * @inheritdoc
     */
    public function shutdown()
    {
        foreach ($this->workers as $i => $worker) {
            $worker->shutdown();
            $this->collect();
        }

        foreach ($this->workers as $i => $worker) {
            $worker->join();
        }

        $this->running = false;
    }

    /**
     * @inheritdoc
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
            $worker = new ForkWorker();
            $worker->setLogger($this->logger);
            $worker->start();

            $this->workers[$next] = $worker;
        }

        return $this->submitTo($next, $task);
    }

    /**
     * @inheritdoc
     */
    public function submitTo($worker, $task)
    {
        $this->logger->info("Submitting to worker.");
        if (!isset($this->workers[$worker])) {
            throw new RuntimeException("The selected worker ({$worker}) does not exist!");
        }

        $this->last = $worker;

        return $this->workers[$worker]->stack($task);
    }

    /**
     * @inheritdoc
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
     * @param ForkStackable $work
     *
     * @return bool
     */
    public function collector(ForkStackable $work)
    {
        if ($work->isTerminated()) {
            $this->logger->warning("Job {$work->getId()} failed and will be removed");
            $this->adaptor->retry($work);
        } elseif ($work->isComplete()) {
            $this->logger->info("Job {$work->getId()} completed successfully");
            $this->adaptor->complete($work);
        } else {
            $this->logger->debug("Requesting more time for job {$work->getId()}");
            $this->adaptor->touch($work);
        }

        return $work->isComplete() || $work->isTerminated();
    }

    /**
     * @inheritdoc
     */
    public function run()
    {
        $this->startTime = time();
        $this->running = true;
        $count = 0;

        while ($this->running) {
            // Check if the pool is at capacity.
            if (($this->size * $this->buffer) > $count) {
                // If the size of the pool is less than the stacked size...
                $stackable = $this->adaptor->listen(5);

                if ($stackable !== null) {
                    // If we received work from the adaptor
                    $this->logger->info("Pool received new job: {$stackable->getId()}");

                    try {
                        $this->submit($stackable);
                    } catch (RuntimeException $e) {
                        $this->logger->critical($e->getMessage(), $e->getTrace());
                    }
                } elseif (count($this->workers) > 0) {
                    // If there is no more work, clean-up workers.
                    $this->logger->debug("Checking " . count($this->workers) . " worker(s) for idle");

                    $workers = array();
                    foreach ($this->workers as $i => $worker) {
                        if ($worker->getStacked() < 1) {
                            if (!$worker->isShutdown()) {
                                $this->logger->info("Shutting down worker {$i} due to idle");
                                if (!$worker->shutdown()) {
                                    $this->logger->warning("Failed to shut down worker {$i}");
                                    $workers[] = $worker;
                                }
                            }
                        } else {
                            $workers[] = $worker;
                        }
                    }
                    $this->workers = $workers;
                }
            } else {
                $this->logger->debug("Pool sleeping");
                //sleep(5);
                usleep(250);
            }

            $count = $this->collect();
        }
    }

    /**
     * @inheritdoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * Set the maximum number of Workers this Pool can create
     *
     * @param int $size
     */
    public function resize($size)
    {
        $this->size = $size;
    }
}
