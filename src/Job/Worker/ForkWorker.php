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
use Legume\Job\StackableInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class ForkWorker
{
    /** @var int $startTime */
    protected $startTime;

    /** @var int $pid */
    private $pid;

    /** @var StackableInterface[] $stack */
    private $stack;

    /** @var bool $running */
    private $running;

    /** @var bool $working */
    private $working;

    /** @var int $size */
    private $size;

    /** @var NullLogger $logger */
    private $logger;

    /** @var string $path */
    protected $path;

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
        if (file_exists("{$this->path}/worker.{$this->pid}")) {
            $fd = fopen("{$this->path}/worker.{$this->pid}", "r+");
            flock($fd, LOCK_SH);

            $buffer = "";
            while (!feof($fd)) {
                $buffer .= fread($fd, 8192);
            }

            $stack = array();
            foreach (unserialize($buffer) as $task) {
                if (!call_user_func($collector, $task)) {
                    $stack[] = $task;
                }
            }

            fseek($fd, 0);
            flock($fd, LOCK_EX);

            fwrite($fd, serialize($stack));
            $size = count($stack);

            flock($fd, LOCK_UN);
            fclose($fd);

            if ($this->isShutdown()) {
                unlink("{$this->path}/worker.{$this->pid}");
                if (count(scandir($this->path)) <= 2) {
                    rmdir($this->path);
                }
            }
        }

        return $size;
    }

    /**
     * @inheritDoc
     */
    public function collector(StackableInterface $task)
    {
        return $task->isComplete();
    }

    /**
     * @inheritDoc
     */
    public function start($path = null)
    {
        if (!is_string($path)) {
            $path = sys_get_temp_dir() . DIRECTORY_SEPARATOR . "legumed." . posix_getpid();
        }

        if (!is_dir($path) && !mkdir($path, 0750)) {
            throw new RuntimeException("Failed to create IPC path: {$path}");
        }
        $this->path = realpath($path);

        $pid = pcntl_fork();
        switch ($pid) {
            case 0: // Child
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

                exit(0);

            case -1: // Error
                $code = pcntl_get_last_error();
                $message = pcntl_strerror($code);
                throw new RuntimeException($message, $code);

            default: // Parent
                $this->pid = $pid;

                $fd = fopen("{$this->path}/worker.{$this->pid}", "w");
                flock($fd, LOCK_EX);
                fwrite($fd, serialize($this->stack));
                flock($fd, LOCK_UN);
                fclose($fd);

                if (!chmod("{$this->path}/worker.{$this->pid}", 0640)) {
                    $this->logger->warning("Failed to chmod worker IPC file", array("{$this->path}/worker.{$this->pid}"));
                }

                $this->logger->debug("Forked worker process", array($this->pid));
        }
    }

    /**
     * @inheritDoc
     */
    public function run()
    {
        while ($this->running) {
            // Sync pending tasks
            $stack = [];
            if (is_readable("{$this->path}/worker.{$this->pid}")) {
                $fd = fopen("{$this->path}/worker.{$this->pid}", "r");
                flock($fd, LOCK_SH);

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

            do {
                $task = array_shift($stack);
            } while($task !== null && $task->isComplete());

            if ($task !== null) {
                $this->working = true;
                $task->run();
                $this->working = false;

                $fd = fopen("{$this->path}/worker.{$this->pid}", "r+");
                flock($fd, LOCK_SH);

                $buffer = "";
                while (!feof($fd)) {
                    $buffer .= fread($fd, 8192);
                }

                $stack = unserialize($buffer);
                foreach ($stack as $i => $work) {
                    /** @var StackableInterface $work */
                    if ($work->getId() == $task->getId()) {
                        $stack[$i] = $task;
                        break;
                    }
                }

                fseek($fd, 0);
                flock($fd, LOCK_EX);

                fwrite($fd, serialize($stack));
                flock($fd, LOCK_UN);
                fclose($fd);
            } else {
                usleep(500 * 1000);
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function stack(&$task)
    {
        $fd = fopen("{$this->path}/worker.{$this->pid}", "r+");
        flock($fd, LOCK_SH);

        $buffer = "";
        while (!feof($fd)) {
            $buffer .= fread($fd, 8192);
        }

        fseek($fd, 0);
        flock($fd, LOCK_EX);

        $stack = unserialize($buffer);
        $stack[] = $task;
        fwrite($fd, serialize($stack));
        flock($fd, LOCK_UN);
        fclose($fd);

        return count($stack);
    }

    /**
     * @inheritDoc
     */
    public function unstack()
    {
        /*
        $fd = fopen("{$this->path}/worker.{$this->pid}", "r+");
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

        return count($buffer);
        */
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function getStacked()
    {
        $size = 0;

        if (file_exists("{$this->path}/worker.{$this->pid}")) {
            $fd = fopen("{$this->path}/worker.{$this->pid}", "r");
            flock($fd, LOCK_SH);

            $buffer = "";
            while (!feof($fd)) {
                $buffer .= fread($fd, 8192);
            }

            flock($fd, LOCK_UN);
            fclose($fd);

            $size = count(unserialize($buffer));
        }

        return $size;
    }

    /**
     * @inheritDoc
     */
    public function isShutdown()
    {
        return !posix_kill($this->pid, 0);
    }

    /**
     * @inheritDoc
     */
    public function isWorking()
    {
        return ($this->getStacked() > 0);
    }

    /**
     * @inheritDoc
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
        $this->logger->debug("Worker {$this->pid} received signal", array($number));
        switch ($number) {
            case SIGHUP:
                $this->running = false;
                break;

            default:
                // Ignore all other signals
        }
    }

    public function isRunning()
    {
        $status = false;
        if (isset($this->pid)) {
            $status = posix_kill($this->pid, 0);
        }

        return $status;
    }
}
