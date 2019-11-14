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

    public function collect($collector = null)
    {
        if (!isset($collector)) {
            $collector = array($this, "collector");
        }

        $size = 0;
        if (is_readable("/tmp/worker.{$this->pid}")) {
            $fd = fopen("/tmp/worker.{$this->pid}", "r+");
            flock($fd, LOCK_EX);

            $buffer = "";
            while (!feof($fd)) {
                $buffer .= fread($fd, 8192);
            }

            if (!empty($buffer)) {
                fseek($fd, 0);
                $stack = unserialize($buffer);
                foreach ($stack as $i => $task) {
                    if (call_user_func($collector, $task)) {
                        unset($stack[$i]);
                    }
                }
                $stack = array_values($stack);

                fwrite($fd, serialize($stack));
                $size = count($stack);
            } else {
                var_dump("No Buffer to collect!");
            }

            flock($fd, LOCK_UN);
            fclose($fd);
        } else {
            var_dump("Not readable!");
        }

        return $size;
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

        /*
        if (socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets) === false) {
            throw new RuntimeException("socket_create_pair failed: " . socket_strerror(socket_last_error()));
        }
        list($child, $parent) = $sockets; // Split the socket into parent / child
        unset($sockets);
        */

        $pid = pcntl_fork();
        switch ($pid) {
            case 0: // Child
                //socket_close($parent);
                //$this->socket = $child;

                $this->pid = posix_getpid();
                $this->startTime = time();
                $this->running = true;

                if (!pcntl_signal(SIGINT, SIG_IGN)) {
                    $this->logger->notice("Failed to ignore SIGTERM handler");
                }

                if (!pcntl_signal(SIGTERM, SIG_IGN)) {
                    $this->logger->notice("Failed to ignore SIGTERM handler");
                }

                if (!pcntl_signal(SIGHUP, [$this, "signal"])) {
                    throw new RuntimeException("Function pcntl_signal() failed");
                }


                $this->logger->debug("Worker process starting", array($this->pid));
                $this->run();

                $this->logger->debug("Worker process complete", array($this->pid));
                unlink("/tmp/worker.{$this->pid}");
                exit(0);

            case -1: // Error
                $code = pcntl_get_last_error();
                $message = pcntl_strerror($code);
                throw new RuntimeException($message, $code);

            default: // Parent
                //socket_close($child);
                //$this->socket = $parent;
                $this->pid = $pid;

                $fd = fopen("/tmp/worker.{$this->pid}", "w");
                flock($fd, LOCK_EX);
                fwrite($fd, serialize($this->stack));
                flock($fd, LOCK_UN);
                fclose($fd);

                $this->logger->debug("Forked worker process", array($this->pid));
        }
    }

    /**
     * @inheritdoc
     */
    public function run()
    {
        while ($this->running) {
            // Sync pending work
            $stack = [];
            if (is_readable("/tmp/worker.{$this->pid}")) {
                $fd = fopen("/tmp/worker.{$this->pid}", "r");
                flock($fd, LOCK_EX);

                $buffer = "";
                while (!feof($fd)) {
                    $buffer .= fread($fd, 8192);
                }

                flock($fd, LOCK_UN);
                fclose($fd);

                if (!empty($buffer)) {
                    $stack = unserialize($buffer);
                }
            }


            /*
            while (($work = $this->ipcReceive()) !== false) {
                if ($work === null) {
                    $work = array_shift($this->stack);
                    $this->logger->debug("Worker unstacking", array($work->getId()));
                } else {
                    $this->stack[] = $work;
                }
            }
            */

            do {
                $work = array_shift($stack);
            } while($work !== null && $work->isComplete());

            if ($work !== null) {
                $this->working = true;
                $work->run();
                $this->working = false;

                $fd = fopen("/tmp/worker.{$this->pid}", "r+");
                flock($fd, LOCK_EX);

                $buffer = "";
                while (!feof($fd)) {
                    $buffer .= fread($fd, 8192);
                }

                $stack = unserialize($buffer);
                foreach ($stack as $i => $task) {
                    if ($task->getId() == $work->getId()) {
                        unset($stack[$i]);
                        $stack[] = $work;
                        break;
                    }
                }

                fseek($fd, 0);
                fwrite($fd, serialize($stack));
                flock($fd, LOCK_UN);
                fclose($fd);
            } else {
                usleep(500 * 1000);
            }
        }
    }

    /**
     * @param StackableInterface $work
     * @return int|void
     */
    public function stack(&$work)
    {
        $fd = fopen("/tmp/worker.{$this->pid}", "r+");
        flock($fd, LOCK_EX);

        $buffer = "";
        while (!feof($fd)) {
            $buffer .= fread($fd, 8192);
        }
        fseek($fd, 0);

        $stack = unserialize($buffer);
        $stack[] = $work;
        fwrite($fd, serialize($stack));
        flock($fd, LOCK_UN);
        fclose($fd);

        return count($stack);
    }

    public function unstack()
    {
        /*
        $this->logger->critical(filesize("/tmp/worker.{$this->pid}"), array(__CLASS__, __FUNCTION__, __LINE__));
        $fd = fopen("/tmp/worker.{$this->pid}", "r+");
        flock($fd, LOCK_EX);

        $buffer = "";
        while (!feof($fd)) {
            $buffer .= fread($fd, 8192);
        }
        var_dump($buffer);
        fseek($fd, 0);

        array_shift($buffer);
        fwrite($fd, serialize($buffer));
        flock($fd, LOCK_UN);
        fclose($fd);
        $this->logger->critical(filesize("/tmp/worker.{$this->pid}"), array(__CLASS__, __FUNCTION__, __LINE__));

        return count($buffer);
        */
        return 0;
    }

    public function getStacked()
    {
        $fd = fopen("/tmp/worker.{$this->pid}", "r");
        flock($fd, LOCK_EX);

        $buffer = "";
        while (!feof($fd)) {
            $buffer .= fread($fd, 8192);
        }

        flock($fd, LOCK_UN);
        fclose($fd);

        $stack = unserialize($buffer);
        return count($stack);
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
        return ($this->getStacked() > 0);
    }

    /**
     * (PECL pthreads &gt;= 2.0.0)<br/>
     * Shuts down the Worker after executing all of the stacked tasks
     * @link https://secure.php.net/manual/en/worker.shutdown.php
     * @return bool <p>Whether the worker was successfully shutdown or not</p>
     */
    public function shutdown()
    {
        return posix_kill($this->pid, SIGHUP);
    }

    public function isJoined()
    {
        return (pcntl_waitpid($this->pid, $status, WNOHANG) > 0);
    }

    public function join()
    {
        pcntl_waitpid($this->pid, $status);
        return pcntl_wifexited($status);
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
        $this->logger->debug("Worker received signal", array($number));
        switch ($number) {
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
}
