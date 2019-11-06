<?php
/* Legume: Multi-thread job manager and daemon for beanstalkd
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
namespace Legume\Job\Worker;

use ArrayAccess;
use Countable;
use Legume\Job\Stackable\ThreadStackable;
use Legume\Job\StackableInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Traversable;
use Worker;

class ForkWorker
{
    /** @var int $jobCount */
    protected $jobCount;

    /** @var int $startTime */
    protected $startTime;

    /** @var int $pid */
    private $pid;

    public function __construct()
    {
		$this->log = new NullLogger();

		$fifo = "/tmp/test.sock";
		if (posix_mkfifo($fifo, 0644)) {
			//$this->fh = fopen($fifo, "r+");
			//stream_set_blocking($this->fh, false);
		}

	}

    public function collect($collector = null)
    {
        // Pool calls the collector
        $fh = fopen("/tmp/test.sock", "r");
		stream_set_blocking($fh, false);
		fclose($fh);
    }

    /**
     * @inheritdoc
     */
    public function run()
    {
        $this->startTime = time();

		while ($this->isRunning()) {
			$work = $this->shift();

			$work->run();

			$this->stack($work);
		}
	}

    /**
     * Use the inherit none option by default.
     *
     * @inheritdoc
     */
    public function start($options = 0)
    {
		$res = pcntl_signal(SIGTERM, [$this, "signal"]);
		$res &= pcntl_signal(SIGINT, [$this, "signal"]);
		$res &= pcntl_signal(SIGHUP, [$this, "signal"]);
		//$res &= pcntl_signal(SIGCHLD, [$this, "signal"]);
		//$res &= pcntl_signal(SIGALRM, array($this, "signal"));
		//$res &= pcntl_signal(SIGTSTP, array($this, "signal"));
		//$res &= pcntl_signal(SIGCONT, array($this, "signal"));

		if (!$res) {
			throw new Exception("Function pcntl_signal() failed!");
		}


		$pid = pcntl_fork();
		switch ($pid) {
			case 0: // Child
				$this->pid = posix_getpid();

				$this->log->notice("Starting worker process: {$this->pid}.");

                $this->run();

				$this->log->notice("Worker process {$this->pid} complete.");
				exit(0);

			case -1: // Error
				$msg = pcntl_strerror(pcntl_get_last_error());
				throw new Exception("Function pcntl_fork() failed: {$msg}");

			default: // Parent
				$this->log->debug("Forked worker process: {$pid}");
		}
    }

    /**
     * @param StackableInterface $work
     * @return int|void
     */
    public function stack(&$work)
    {
		$fh = fopen("/tmp/test.sock", "r+");

		$stack = unserialize(stream_get_contents($fh));
		$size = array_push($stack, $work);
		file_put_contents($fh, serialize($stack));

		fclose($fh);

		return $size;
    }

    public function unstack()
    {
    	/*
		$fh = fopen("/tmp/test.sock", "r+");

		$stack = unserialize(stream_get_contents($fh));
		array_shift($stack);
		$size = count($stack);
		file_put_contents($fh, serialize($stack));

		fclose($fh);
    	*/
    	$this->shift();

		return $this->getStacked();
    }

	/**
	 * @return StackableInterface|null
	 */
	public function shift()
	{
		$fh = fopen("/tmp/test.sock", "r+");

		$stack = unserialize(stream_get_contents($fh));
		$work = array_shift($stack);
		file_put_contents($fh, serialize($stack));

		fclose($fh);

		return $work;
	}

    public function getStacked()
    {
		$fh = fopen("/tmp/test.sock", "r");

		/** @var ThreadStackable[] $work */
		$stack = unserialize(stream_get_contents($fh));

		fclose($fh);


		return count($stack);
    }

	/**
	 * (PECL pthreads &gt;= 2.0.0)<br/>
	 * Whether the worker has been shutdown or not
	 * @link https://secure.php.net/manual/en/worker.isshutdown.php
	 * @return bool <p>Returns whether the worker has been shutdown or not</p>
	 */
	public function isShutdown() {}

	/**
	 * (PECL pthreads &lt; 3.0.0)<br/>
	 * Tell if a Worker is executing Stackables
	 * @link https://secure.php.net/manual/en/worker.isworking.php
	 * @return bool <p>A boolean indication of state</p>
	 */
	public function isWorking() {}

	/**
	 * (PECL pthreads &gt;= 2.0.0)<br/>
	 * Shuts down the Worker after executing all of the stacked tasks
	 * @link https://secure.php.net/manual/en/worker.shutdown.php
	 * @return bool <p>Whether the worker was successfully shutdown or not</p>
	 */
	public function shutdown()
	{
		posix_kill($this->pid, SIGHUP);
	}


    /**
     * Sets a logger instance on the object.
     *
     * @param LoggerInterface $logger
     *
     * @return void
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->log = $logger;
    }
}
