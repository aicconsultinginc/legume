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
use Legume\Job\Stackable;
use Legume\Job\StackableInterface;
use Legume\Job\Worker\ThreadWorker;
use Pool;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;
use Threaded;

class ThreadPool extends Pool implements ManagerInterface
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

    /** @var int $startTime */
    protected $startTime;

    /** @var int $timeout */
    protected $timeout = 5;

    /** @var ThreadWorker[int] */
    protected $workers;

    /**
     * @inheritDoc
     */
    public function __construct(QueueAdaptorInterface $adaptor)
    {
        parent::__construct(1, ThreadWorker::class, array());

        $this->adaptor = $adaptor;
        $this->logger = new NullLogger();
        $this->running = false;

        $this->workers = array();
        $this->last = 0;
    }

    /**
     * @inheritDoc
     */
    public function shutdown()
    {
        // Cleanup the workers and unstack jobs.
        parent::shutdown();

        $this->collect();
        $this->running = false;
    }

    /**
     * @inheritDoc
     */
    public function submit($task)
    {
        $task = Threaded::extend($task);

        $next = 0;
        if ($this->size > 0) {
            $next = ($this->last + 1) % $this->size;
            if (isset($this->workers[$next])) {
                // If our next worker exists,
                // Find the worker with the least amount of work.
                foreach ($this->workers as $i => $worker) {
                    if ($worker->getStacked() < $this->workers[$next]->getStacked()) {
                        $next = $i;
                    }
                }
            }
        }

        if (!isset($this->workers[$next])) {
            $this->workers[$next] = new $this->class(...$this->ctor);
            $this->workers[$next]->setLogger($this->logger);
            $this->workers[$next]->start();
        }

        return $this->submitTo($next, $task);
    }

    /**
     * @inheritDoc
     */
    public function submitTo($worker, $task)
    {
        if (!isset($this->workers[$worker])) {
            throw new RuntimeException("The selected worker ({$worker}) does not exist!");
        }

        $this->last = $worker;
        //$this->work[] = $task;
        return $this->workers[$worker]->stack($task);
    }

    /**
     * @inheritDoc
     */
    public function collect($collector = null)
    {
        if ($collector == null) {
            $collector = array($this, "collector");
        }

        $count = 0;
        foreach ($this->workers as $worker) {
            $count += $worker->collect($collector);
        }

        return $count;
    }

    /**
     * @param StackableInterface $work
     *
     * @return bool
     */
    public function collector(StackableInterface $work)
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
     * @inheritDoc
     */
    public function run()
    {
        $this->running = true;
        $this->startTime = time();

        $count = 0;
        while ($this->running) {
            if ($this->size > $count) {
                $stackable = $this->adaptor->listen(5);

                if ($stackable !== null) {
                    $this->logger->info("Pool received new job: {$stackable->getId()}");

                    try {
                        $this->submit($stackable);
                    } catch (RuntimeException $e) {
                        $this->logger->error($e->getMessage(), $e->getTrace());
                    }
                } elseif (count($this->workers) > 0) {
                    // If there is no more work, clean-up works.
                    $this->logger->debug("Checking " . count($this->workers) . " worker(s) for idle.");

                    $workers = array();
                    foreach ($this->workers as $i => $worker) {
                        $stacked = $worker->getStacked();
                        if ($stacked < 1) {
                            if (!$worker->isShutdown()) {
                                $this->logger->debug("Pool shutting down worker due to idle", array($i));
                                $worker->shutdown();
                                $workers[] = $worker;
                            } else {
                                if ($worker->isJoined()) {
                                    $this->logger->debug("Pool cleaning up worker", array($i));
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
                usleep(250);
            }

            $count = $this->collect();
        }
    }

    /**
     * @inheritDoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
