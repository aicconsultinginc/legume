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

use Exception;
use Legume\Job\Stackable\ForkStackable;
use RuntimeException;
use Legume\Job\StackableInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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

    private $socket;

    /** @var bool $running */
    private $running;

    /** @var bool $working */
    private $working;

    /** @var int $size */
    private $size;

    /** @var NullLogger $logger */
    private $logger;

    public function __construct()
    {
        $this->logger = new NullLogger();
        $this->stack = array();
        $this->size = 0;
        $this->running = false;
        $this->working = false;
    }

    public function __destruct()
    {
        socket_close($this->socket);
    }

    public function collect($collector = null)
    {
        if (!isset($collector)) {
            $collector = array($this, "collector");
        }

        while (($work = $this->ipcReceive()) !== false) {
            if (call_user_func($collector, $work)) {
                $this->size--;
            }
        }

        return $this->size;
    }

    /**
     * @param ForkStackable $work
     *
     * @return bool
     */
    public function collector(ForkStackable $work)
    {
        return $work->isComplete() || $work->isTerminated();
    }

    /**
     * Use the inherit none option by default.
     *
     * @inheritdoc
     */
    public function start($options = 0)
    {
        if (socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets) === false) {
            throw new RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // Split the socket into parent / child
        unset($sockets);

        $pid = pcntl_fork();
        switch ($pid) {
            case 0: // Child
                socket_close($parent);
                $this->socket = $child;

                $this->pid = posix_getpid();
                $this->startTime = time();
                $this->running = true;

                if (!pcntl_signal(SIGCHLD, SIG_DFL)) {
                    $this->logger->notice("Failed to unregister SIGCHLD handler");
                }

                $res = pcntl_signal(SIGTERM, [$this, "signal"]);
                $res &= pcntl_signal(SIGINT, [$this, "signal"]);
                $res &= pcntl_signal(SIGHUP, [$this, "signal"]);

                if (!$res) {
                    throw new RuntimeException("Function pcntl_signal() failed");
                }


                $this->logger->debug("Worker process starting", array($this->pid));
                $this->run();

                if (!pcntl_signal(SIGCHLD, SIG_DFL)) {
                    $this->logger->notice("Failed to unregister SIGCHLD handler");
                }

                $this->logger->debug("Worker process complete", array($this->pid));
                exit(0);

            case -1: // Error
                $code = pcntl_get_last_error();
                $message = pcntl_strerror($code);
                throw new Exception($message, $code);

            default: // Parent
                socket_close($child);
                $this->socket = $parent;
                $this->pid = $pid;

                $this->logger->debug("Forked worker process", array($this->pid));
        }
    }

    /**
     * @inheritdoc
     * @throws Exception
     */
    public function run()
    {
        while ($this->running) {
            // Transfer pending work
            while (($work = $this->ipcReceive()) !== false) {
                if ($work === null) {
                    $work = array_shift($this->stack);
                    $this->logger->notice("Worker unstacking", array($work->getId()));
                } else {
                    $this->stack[] = $work;
                }
            }

            $work = array_shift($this->stack);
            if ($work !== null) {
                $this->working = true;
                $work->run();
                $this->working = false;

                // Returned the processed work back to the pool
                $this->ipcSend($work);

                // Sync the remaining stacked work
                foreach ($this->stack as $work) {
                    $this->ipcSend($work);
                }
            } else {
                $this->logger->notice("Worker sleeping", array($this->pid));
                usleep(250);
            }
        }
    }

    /**
     * @param StackableInterface $work
     * @return int|void
     */
    public function stack(&$work)
    {
        $this->ipcSend($work);

        return ++$this->size;
    }

    public function unstack()
    {
        $this->ipcSend(null);

        return $this->size;
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
    public function isShutdown()
    {
        return !posix_kill($this->pid, 0);
    }

    /**
     * (PECL pthreads &lt; 3.0.0)<br/>
     * Tell if a Worker is executing Stackables
     * @link https://secure.php.net/manual/en/worker.isworking.php
     * @return bool <p>A boolean indication of state</p>
     */
    public function isWorking()
    {

    }

    /**
     * (PECL pthreads &gt;= 2.0.0)<br/>
     * Shuts down the Worker after executing all of the stacked tasks
     * @link https://secure.php.net/manual/en/worker.shutdown.php
     * @return bool <p>Whether the worker was successfully shutdown or not</p>
     */
    public function shutdown()
    {
        $success = false;

        if (posix_kill($this->pid, SIGHUP)) {
            pcntl_waitpid($this->pid, $status);
            $success = pcntl_wifexited($status);
        }

        return $success;
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
        $this->logger = $logger;
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
        $this->logger->info("Worker received signal", array($number));
        switch ($number) {
            case SIGINT:
            case SIGTERM:
            case SIGHUP:
                while (($work = array_shift($this->stack)) !== null) {
                    $this->logger->debug("Worker dropping job", array($work->getId()));
                }

                $this->running = false;
                break;

            default:
                // handle all other signals
        }
    }

    public function isRunning()
    {
        // TODO What if pid is == 0?
        return posix_kill($this->pid, 0);
    }


    // https://github.com/lifo101/php-ipc/blob/master/src/Lifo/IPC/ProcessPool.php

    private function ipcSend($data)
    {
        $serialized = serialize($data);
        $hdr = pack('N', strlen($serialized));    // 4 byte length
        $buffer = $hdr . $serialized;
        $total = strlen($buffer);
        while (true) {
            $sent = socket_write($this->socket, $buffer);
            if ($sent === false) {
                $this->logger->error(socket_strerror(socket_last_error()));
                break;
            }
            if ($sent >= $total) {
                break;
            }
            $total -= $sent;
            $buffer = substr($buffer, $sent);
        }
    }

    private function ipcReceive()
    {
        $data = false;
        $sockets = array($this->socket);
        $ok = @socket_select($sockets, $unused, $unused, 0);
        if ($ok !== false && $ok > 0) {
            $socket = array_shift($sockets);
            // read 4 byte length first
            $hdr = '';
            do {
                $read = socket_read($socket, 4 - strlen($hdr));
                if ($read === false || $read === '') {
                    return false;
                }
                $hdr .= $read;
            } while (strlen($hdr) < 4);

            list($len) = array_values(unpack("N", $hdr));
            // read the full buffer
            $buffer = '';
            do {
                $read = socket_read($socket, $len - strlen($buffer));
                if ($read === false || $read == '') {
                    return false;
                }
                $buffer .= $read;
            } while (strlen($buffer) < $len);

            $data = unserialize($buffer);
        }

        return $data;
    }
}
