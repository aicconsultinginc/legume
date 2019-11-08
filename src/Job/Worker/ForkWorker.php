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
		/*
		if (posix_mkfifo($fifo, 0644)) {
			$fh = fopen($fifo, "w+");
			stream_set_blocking($fh, false);
			$i = fwrite($fh, serialize(array()));
			fclose($fh);
			$this->log->notice("Put file content $i");
        }
		*/

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

        $this->log->debug("Create stream.");
        $sockets = array();
        $domain = strtoupper(substr(PHP_OS, 0, 3)) == 'WIN' ? AF_INET : AF_UNIX;
        if (socket_create_pair($domain, SOCK_STREAM, 0, $sockets) === false) {
            throw new \RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // just to make the code below more readable
        unset($sockets);

		$pid = pcntl_fork();
		switch ($pid) {
			case 0: // Child
                socket_close($child);
                $this->socket = $parent;
				$this->pid = posix_getpid();

				$this->log->notice("Starting worker process: {$this->pid}.");
                //$this->run();

                //stream_filter_prepend($this->socket , "FifoStreamFilter");
                //stream_set_blocking($fh, false);

                while (true) {
                    // Send the result back to the parent.
                    self::socket_send($parent, $result);

                    $this->log->notice("Worker sleeping...");
                    sleep(5);
                }

                stream_socket_shutdown($this->socket);

				$this->log->notice("Worker process {$this->pid} complete.");
				exit(0);

			case -1: // Error
				$msg = pcntl_strerror(pcntl_get_last_error());
				throw new Exception("Function pcntl_fork() failed: {$msg}");

			default: // Parent
                socket_close($parent);
				$this->log->debug("Forked worker process: {$pid}");

                $this->log->debug("Saving child socket");
                $this->socket = $child;
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
        //$fh = fopen("/tmp/test.sock", "a");
        //stream_set_blocking($fh, false);
        //fwrite($this->socket, serialize($work));
        //fflush($this->socket);

		stream_socket_sendto($this->socket, serialize($work)."\0");

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
		return 1;
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

    // https://github.com/lifo101/php-ipc/blob/master/src/Lifo/IPC/ProcessPool.php
    public static function socket_send($socket, $data)
    {
        $serialized = serialize($data);
        $hdr = pack('N', strlen($serialized));    // 4 byte length
        $buffer = $hdr . $serialized;
        $total = strlen($buffer);
        while (true) {
            $sent = socket_write($socket, $buffer);
            if ($sent === false) {
                // @todo handle error?
                //$error = socket_strerror(socket_last_error());
                break;
            }
            if ($sent >= $total) {
                break;
            }
            $total -= $sent;
            $buffer = substr($buffer, $sent);
        }
    }


    public static function socket_fetch($socket)
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
