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
use Legume\Job\Stackable\ForkStackable;
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

    /** @var ForkStackable[] $stack */
    private $stack;

    /** @var int $size */
    private $size;

	private $socket;

	public function __construct()
    {
		$this->log = new NullLogger();
		$this->stack = array();
	}

	public function __destruct()
	{
		socket_close($this->socket);
	}

	public function collect($collector = null)
    {
		socket_set_nonblock($this->socket);
		while (($work = $this->socket_fetch($this->socket)) !== null) {
			$this->log->notice("Collector Fetched", array($work));

			if (call_user_func($collector, $work)) {
				$this->size--;
			}
		}
		socket_set_block($this->socket);

		return $this->size;
	}


    /**
     * @inheritdoc
     */
    public function run()
    {
		while ($this->isRunning()) {
			socket_set_nonblock($this->socket);
			while (($work = $this->socket_fetch($this->socket)) !== null) {
				$this->log->notice("Socket Fetched");
				$this->stack[] = $work;
			}
			socket_set_block($this->socket);
			$this->log->notice("While done");

			foreach ($this->stack as $work) {
				$this->socket_send($this->socket, $work);
				$this->log->notice("Socket Touch");
			}

			$this->log->notice("Worker processing...");
			$work = array_shift($this->stack);
			if ($work) {
				$work->run();

				$this->socket_send($this->socket, $work);
				$this->log->notice("Socket Sent");
			} else {
				$this->log->notice("Out of work");
				sleep(1);
			}
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


        $this->log->debug("Create stream.");
        //$domain = strtoupper(substr(PHP_OS, 0, 3)) == 'WIN' ? AF_INET : AF_UNIX;
        if (socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets) === false) {
            throw new RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // just to make the code below more readable
        unset($sockets);

		$pid = pcntl_fork();
		switch ($pid) {
			case 0: // Child
				socket_close($parent);
				$this->socket = $child;
				$this->pid = posix_getpid();

				$res = pcntl_signal(SIGCHLD, [$this, "signal"]);
				$res &= pcntl_signal(SIGHUP, [$this, "signal"]);

				//$res = pcntl_signal(SIGTERM, [$this, "signal"]);
				//$res &= pcntl_signal(SIGINT, [$this, "signal"]);
				//$res &= pcntl_signal(SIGCHLD, [$this, "signal"]);
				//$res &= pcntl_signal(SIGALRM, array($this, "signal"));
				//$res &= pcntl_signal(SIGTSTP, array($this, "signal"));
				//$res &= pcntl_signal(SIGCONT, array($this, "signal"));

				if (!$res) {
					throw new RuntimeException("Function pcntl_signal() failed!");
				}


				$this->log->notice("Starting worker process: {$this->pid}.");
                $this->run();

				socket_close($this->socket);

                if (!pcntl_signal(SIGCHLD, SIG_DFL)) {
					$this->log->notice("Failed to unregister SIGCHLD handler.");
				}

				$this->log->notice("Worker process {$this->pid} complete.");
				exit(0);

			case -1: // Error
				$msg = pcntl_strerror(pcntl_get_last_error());
				throw new Exception("Function pcntl_fork() failed: {$msg}");

			default: // Parent
				socket_close($child);
				$this->socket = $parent;
				$this->pid = $pid;

				$this->log->debug("Forked worker process: {$pid}");

                $this->log->debug("Saving child socket");
		}
    }

    /**
     * @param StackableInterface $work
     * @return int|void
     */
    public function stack(&$work)
    {
        $this->log->debug("Stacking work...");

		$this->socket_send($this->socket, $work);

		$this->log->debug("Work stacked...");

		return ++$this->size;
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
		//$fh = fopen("/tmp/test.sock", "r+");
        $this->log->error("Get Stack");
		//$stack = unserialize(stream_get_contents($fh));
		$this->log->error("Got Stack");
		$work = array_shift($stack);
		var_dump($work);
		fwrite($this->socket, serialize($stack));

		return $work;
	}

    public function getStacked()
    {
		return $this->size;
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
            */

            case SIGHUP:
            	// Stop accepting new jobs.
				socket_shutdown($this->socket, 0);
                break;

            default:
                // handle all other signals
        }
    }

    public function isRunning()
    {
        return true;
    }

    // https://github.com/lifo101/php-ipc/blob/master/src/Lifo/IPC/ProcessPool.php
    public function socket_send($socket, $data)
    {
        $serialized = serialize($data);
        $hdr = pack('N', strlen($serialized));    // 4 byte length
        $buffer = $hdr . $serialized;
        $total = strlen($buffer);
        while (true) {
            $sent = socket_write($socket, $buffer);
            if ($sent === false) {
				$this->log->error(socket_strerror(socket_last_error()));
                break;
            }
            if ($sent >= $total) {
                break;
            }
            $total -= $sent;
            $buffer = substr($buffer, $sent);
        }
    }


    public function socket_fetch($socket)
    {
        // read 4 byte length first
        $hdr = '';
        do {
            $read = socket_read($socket, 4 - strlen($hdr));
            if ($read === false or $read === '') {
                return null;
            }
            $hdr .= $read;
        } while (strlen($hdr) < 4);
        list($len) = array_values(unpack("N", $hdr));
        // read the full buffer
        $buffer = '';
        do {
            $read = socket_read($socket, $len - strlen($buffer));
            if ($read === false or $read == '') {
                return null;
            }
            $buffer .= $read;
        } while (strlen($buffer) < $len);
        $data = unserialize($buffer);
        return $data;
    }
}
