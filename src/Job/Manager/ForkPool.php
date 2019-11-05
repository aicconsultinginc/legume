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
use Legume\Job\ThreadStackable;
use Legume\Job\Worker\ForkWorker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class ForkPool implements ManagerInterface
{
    /** @var QueueAdaptorInterface $adaptor */
    protected $adaptor;

    /** @var LoggerInterface $log */
    protected $log;

    /** @var boolean $running */
    protected $running;

	/** @var int $size */
	protected $size;

    /** @var int $startTime */
    protected $startTime;

    /** @var Threaded[int] */
    protected $work;

	/** @var ForkWorker[int] */
	protected $workers;

    /** @var int $last */
	protected $last;

	/**
	 * @param QueueAdaptorInterface $adaptor
	 */
    public function __construct(QueueAdaptorInterface $adaptor)
    {
        //parent::__construct(1, ThreadWorker::class, array($autoload));

        $this->adaptor = $adaptor;
		$this->log = new NullLogger();
        $this->running = false;
        $this->work = array();

        $this->workers = array();
        $this->last = 0;
    }

    /**
     * @inheritdoc
     */
	public function shutdown()
	{
		$this->collect();
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
				foreach ($this->workers as $i => $worker) {
					if ($worker->getStacked() < $this->workers[$next]->getStacked()) {
						$next = $i;
					}
				}
			}
		}

		if (!isset($this->workers[$next])) {
			$this->workers[$next] = new ForkWorker();
			$this->workers[$next]->setLogger($this->log);
			$this->workers[$next]->start();
		}

		return $this->submitTo($next, $task);
    }

    /**
     * @inheritdoc
     */
    public function submitTo($worker, $task)
    {
        if (!isset($this->workers[$worker])) {
            throw new RuntimeException("The selected worker ({$worker}) does not exist!");
        }

        $this->last = $worker;
        $this->work[] = $task;

        return $this->workers[$worker]->stack($task);
    }

    /**
     * @inheritdoc
     */
    public function collect($collector = null)
    {
        if ($collector == null) {
            $collector = array($this, "collector");
        }

        // Create a temp work buffer to reorder preserved work.
        $work = array();
        foreach ($this->work as $i => $task) {
            if (!call_user_func($collector, $task)) {
                $work[] = $this->work[$i];
            }
        }
        $this->work = $work;

        return count($this->work);
    }

    /**
     * @param ThreadStackable $work
     *
     * @return bool
     */
    protected function collector(ThreadStackable $work)
    {
        $complete = $work->isComplete();
        if ($complete) {
            if ($work->isTerminated()) {
                $this->log->warning("Job {$work->getId()} failed and will be submitted for retry!");
                $this->adaptor->retry($work);
            } else {
                $this->log->info("Job {$work->getId()} completed successfully.");
                $this->adaptor->complete($work);
            }
		} else {
            $this->log->debug("Requesting more time for job {$work->getId()}.");
            $this->adaptor->touch($work);
        }

        return $complete;
    }

	/**
	 * @inheritdoc
	 */
	public function run()
	{
		$this->running = true;
		$this->startTime = time();

		while ($this->running) {
			if ($this->size > count($this->work)) {
				$stackable = $this->adaptor->listen(5);

				if ($stackable !== null) {
					$this->log->info("Pool received new job: {$stackable->getId()}");

					try {
						$this->submit($stackable);
					} catch (RuntimeException $e) {
						$this->log->warning($e->getMessage());
					}
				} elseif (count($this->workers) > 0) {
					// If there is no more work, clean-up works.
					$this->log->debug("Checking " . count($this->workers) . " worker(s) for idle.");

					$workers = array();
					foreach ($this->workers as $i => $worker) {
						$stacked = $worker->getStacked();
						if ($stacked < 1) {
							if (! $worker->isShutdown()) {
								$this->log->info("Shutting down worker {$i} due to idle.");
								$worker->shutdown();
								$workers[] = $worker;
							} else if ($worker->isJoined()) {
								$this->log->info("Cleaning up worker {$i}.");
							}
						} else {
							$workers[] = $worker;
						}
					}
					$this->workers = $workers;
				}
			} else {
				$this->log->debug("Sleeping...");
				sleep(1);
			}

			$this->collect();
		}
	}

    /**
     * @inheritdoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->log = $logger;
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
