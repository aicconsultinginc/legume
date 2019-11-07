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
use Legume\Job\FifoStreamFilter;
use RuntimeException;
use Legume\Job\Stackable\ThreadStackable;
use Legume\Job\StackableInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Traversable;

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
        $this->startTime = time();

        $fifo = "/tmp/test.sock";
        if (posix_mkfifo($fifo, 0644)) {
            /*
            $fh = fopen($fifo, "w+");
            stream_set_blocking($fh, false);
            $i = fwrite($fh, serialize(array()));
            fclose($fh);
            $this->log->notice("Put file content $i");
            */
        }

        stream_filter_register("FifoStreamFilter", FifoStreamFilter::class);
        $sock = stream_socket_client("unix://{$fifo}", $errno, $errst);

		$res = pcntl_signal(SIGTERM, [$this, "signal"]);
		$res &= pcntl_signal(SIGINT, [$this, "signal"]);
		$res &= pcntl_signal(SIGHUP, [$this, "signal"]);
		//$res &= pcntl_signal(SIGCHLD, [$this, "signal"]);
		//$res &= pcntl_signal(SIGALRM, array($this, "signal"));
		//$res &= pcntl_signal(SIGTSTP, array($this, "signal"));
		//$res &= pcntl_signal(SIGCONT, array($this, "signal"));

		if (!$res) {
			throw new RuntimeException("Function pcntl_signal() failed!");
		}


		$pid = pcntl_fork();
		switch ($pid) {
			case 0: // Child
				$this->pid = posix_getpid();

				$this->log->notice("Starting worker process: {$this->pid}.");
                //$this->run();





                stream_filter_prepend($sock, "FifoStreamFilter");
                //stream_set_blocking($fh, false);

                while (true) {
                    $this->log->notice("Test1", [filesize($fifo)]);
                    $contents = fread($sock, 10);
                    $this->log->notice("Test2", [$contents]);
                    sleep(5);
                }

                fclose($sock);

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
        /*
		$fh = fopen("/tmp/test.sock", "r+");
        stream_set_blocking($fh, false);

        var_dump(stream_get_contents($fh));
		$stack = unserialize(stream_get_contents($fh));
		$size = array_push($stack, $work);
		fwrite($fh, serialize($stack));

		fclose($fh);
        */
        $this->log->debug("Stacking work...");
        $fh = fopen("/tmp/test.sock", "a");
        //stream_set_blocking($fh, false);
        fwrite($fh, serialize($work));
        fflush($fh);
        fclose($fh);
        $this->log->debug("Work stacked...");

		return 1;
    }

    public function unstack()
    {
    	/*
		$fh = fopen("/tmp/test.sock", "r+");

		$stack = unserialize(stream_get_contents($fh));
		array_shift($stack);
		$size = count($stack);
		fwrite($fh, serialize($stack));

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
        $this->log->error("Get Stack");
		$stack = unserialize(stream_get_contents($fh));
		$this->log->error("Got Stack");
		$work = array_shift($stack);
		var_dump($work);
		fwrite($fh, serialize($stack));

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
	public function isWorking() {

    }

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


    /**
     * Signal handler for child process signals.
     *
     * @param int $number
     * @param mixed $info
     *
     * @throws Exception
     */
    public function signal($number, $info = null)
    {
        $this->log->info("Worker received signal: {$number}");

        switch ($number) {
            case SIGTERM:
                break;
            /*
            case SIGINT:
                foreach ($this->workers as $worker) {
                    while($worker->shift());
                    $this->collect();
                }
            case SIGTERM:
                $this->shutdown();
                break;

            case SIGHUP:
                $this->shutdown();
                break;
            */
            default:
                // handle all other signals
        }
    }

    public function isRunning()
    {
        return true;
    }
}
