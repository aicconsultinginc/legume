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
namespace Legume;

use Exception;
use GetOpt\Command;
use GetOpt\GetOpt;
use GetOpt\Option;
use Legume\Job\Manager\ForkPool;
use Legume\Job\Manager\ThreadPool;
use Legume\Job\ManagerInterface;
use Legume\Job\QueueAdapter\PheanstalkAdapter;
use Monolog\Handler\NullHandler;
use Monolog\Handler\SyslogHandler;
use Monolog\Logger;
use Pheanstalk\Pheanstalk;
use Psr\Container\ContainerInterface as DI;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;

class Daemon implements LoggerAwareInterface
{
	/** @var DI $container */
	protected $container;

	/** @var Logger $log */
	protected $log;

	/** @var GetOpt $opts */
	private $opts;

	/** @var ManagerInterface $pool */
	protected $pool;

	public function __construct()
	{
		$this->container = new Container();
		$this->loadDependencies($this->container);

		$this->log = new Logger(basename($_SERVER["SCRIPT_FILENAME"], ".php"));
		$this->opts = $this->createOpts();
		$this->pool = null;
	}

	/**
	 * @return GetOpt
	 */
	private function createOpts()
	{
		$queue = array(
			Option::create('h', "host", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("Hostname or IP address of the job queue")
				->setDefaultValue("localhost"),

			Option::create('p', "port", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("Port number of the job queue")
				->setValidation("is_numeric"),

			Option::create('t', "timeout", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("The socket timeout for connecting to the job queue")
				->setValidation("is_numeric"),
		);

		$manager = array(
			Option::create('j', "jobs", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("A PHP file returning an array map of available jobs.")
				->setValidation("file_exists"),

			Option::create('s', "size", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("The maximum number of parallel jobs to managed by the pool")
				->setValidation("is_numeric")
				->setDefaultValue(1),
		);

		$suExec = array(
			Option::create('u', "user", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("Username to suExec the job manager process"),

			Option::create('g', "group", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("Groupname to suExec the job manager process"),

			Option::create('n', "nice", GetOpt::REQUIRED_ARGUMENT)
				->setDescription("The system priority for the job manager process process")
		);

		$daemon = array(
			Option::create('D', "daemon")
				->setDescription("Run the job manager as a background process")
				->setDefaultValue(false)
		);

		$opts = new GetOpt();
		$opts->addCommands(array(
				Command::create("start", [$this, "start"])
					->setDescription("Start the Legume job manager")
					->addOptions(array_merge($queue, $manager, $suExec, $daemon)),

				Command::create("stop", [$this, "stop"])
					->setDescription('Stop a job manager background process'),
			))
			->addOptions(array(
				Option::create('H', "help")
					->setDescription("Display a command contextual help"),

				Option::create('P', "pid", GetOpt::REQUIRED_ARGUMENT)
					->setDescription("A file to store the pid of the job manager process"),

				Option::create('v', "verbose", GetOpt::OPTIONAL_ARGUMENT)
					->setDescription("Set the verbosity of the job manager")
					->setValidation("is_numeric")
					->setValue(5)
			));

		return $opts;
	}

	/**
	 * @param DI $container
	 */
	protected function loadDependencies(DI &$container)
	{
		$container[Pheanstalk::class] = function (DI $container) {
			$host = $this->opts->getOption("host");
			$port = $this->opts->getOption("port");
			$timeout = $this->opts->getOption("timeout");

			$client = new Pheanstalk($host, $port, $timeout);

			return $client;
		};


		$container[PheanstalkAdapter::class] = function (DI $container) {
			$adaptor = new PheanstalkAdapter($container[Pheanstalk::class]);
			$adaptor->setLogger($this->log);

			$jobs = array("Example" => Job\Handler\Example::class);
			$jobFile = $this->opts->getOption("jobs");
			if ($jobFile !== null) {
				$jobs = include($jobFile);
			}

			foreach ($jobs as $name => $callable) {
				$adaptor->register($name, $callable);
			}
			$adaptor->unregister(Pheanstalk::DEFAULT_TUBE);

			return $adaptor;
		};


		$container[ThreadPool::class] = function (DI $container) {
			/** @var PheanstalkAdapter $adapter */
			$adapter = $container->get(PheanstalkAdapter::class);

			/** @var int $size */
			$size = $this->opts->getOption("size");

			$pool = new ThreadPool($adapter);
			$pool->setLogger($this->log);
			$pool->resize($size);

			return $pool;
		};

        $container[ForkPool::class] = function (DI $container) {
            /** @var PheanstalkAdapter $adapter */
            $adapter = $container->get(PheanstalkAdapter::class);

            /** @var int $size */
            $size = $this->opts->getOption("size");

            $pool = new ForkPool($adapter);
            $pool->setLogger($this->log);
            $pool->resize($size);

            return $pool;
        };
	}

	/**
	 * @return int|bool
	 * @throws Exception
	 */
	private function readPid()
	{
		$pid = false;
		$pidFile = $this->opts->getOption("pid");
		if ($pidFile !== null) {
			if (!file_exists($pidFile)) {
				throw new Exception("The pid file does not exists!");
			}

			$pid = file_get_contents($pidFile);
		}

		return $pid;
	}

	/**
	 * @param int $pid
	 *
	 * @return bool
	 * @throws Exception
	 */
	private function writePid($pid)
	{
		$success = false;
		$pidFile = $this->opts->getOption("pid");
		if ($pidFile !== null) {
			if (file_exists($pidFile)) {
				throw new Exception("The pid file already exists!");
			}

			$success = (file_put_contents($pidFile, $pid) > 0);
		}

		return $success;
	}

	/**
	 * @param int $pid
	 *
	 * @return bool
	 * @throws Exception
	 */
	private function removePid()
	{
		$success = false;
		$pidFile = $this->opts->getOption("pid");
		if ($pidFile !== null) {
			if (! unlink($pidFile)) {
				throw new Exception("Failed to remove pid file!");
			}

			$success = true;
		}

		return $success;
	}

	private function suExec()
	{
		$niceness = $this->opts->getOption("nice");
		if ($niceness !== null) {
			if (!pcntl_setpriority($niceness, getmypid())) {
				$this->log->alert("Failed to set processor priority.");
			}
		}

		$group = $this->opts->getOption("group");
		if ($group !== null) {
			/** @var array $groupInfo */
			$groupInfo = posix_getgrnam($group);
			if (!isset($groupInfo["gid"])) {
				$this->log->alert("Invalid process group name.");
			} else if (!posix_setgid($groupInfo["gid"])) {
				$this->log->alert("Failed to change processor group.");
			}
		}

		$user = $this->opts->getOption("user");
		if ($user !== null) {
			/** @var array $userInfo */
			$userInfo = posix_getpwnam($user);
			if (!isset($userInfo["uid"])) {
				$this->log->alert("Invalid process user name.");
			} else if (!posix_setuid($userInfo["uid"])) {
				$this->log->alert("Failed to change pool user.");
			}
		}
	}

    /**
     * @throws Exception
     */
	protected function start()
    {
		$this->pool = $this->container->get(ForkPool::class);

		if (function_exists("pcntl_async_signals")) {
			pcntl_async_signals(true);
		} else {
			declare(ticks = 1);
		}

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

		$daemon = $this->opts->getOption("daemon");
		if ($daemon !== null) {
			$pid = pcntl_fork();
			switch ($pid) {
				case 0: // Child
					$childPid = posix_setsid();
					$this->suExec();

					$this->log->notice("Starting daemon process: {$childPid}.");
					$this->pool->run();
					$this->log->notice("Daemon process {$childPid} complete.");
					exit(0);

				case -1: // Error
					$msg = pcntl_strerror(pcntl_get_last_error());
					throw new Exception("Function pcntl_fork() failed: {$msg}");

				default: // Parent
					$this->log->debug("Forked worker process: {$pid}");
					if (!$this->writePid($pid)) {
						posix_kill($pid, SIGTERM);
						throw new Exception("Failed to create pid file for the daemon process!");
					}
			}
		} else {
			$pid = posix_getpid();
			if (!$this->writePid($pid)) {
				throw new Exception("Failed to create pid file for the daemon process!");
			}

			$this->suExec();

			$this->log->notice("Starting process: {$pid}.");
			$this->pool->run();
			$this->log->notice("Process {$pid} complete.");

			$this->removePid();
			exit(0);
		}
    }

	/**
	 * @return int
	 * @throws Exception
	 */
    public function stop()
    {
		$pid = $this->readPid();
		if ($pid === false) {
			throw new Exception("Missing or invalid pid file provided");
		}

        $this->log->notice("Sending shutdown signal to daemon: {$pid}\n");
        posix_kill($pid, SIGTERM);
        $status = posix_get_last_error();
        if ($status == 0) {
			$this->removePid();
		}

        return $status;
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
        $this->log->info("Process received signal: {$number}");

        switch ($number) {
            case SIGTERM:
			case SIGINT:
				$this->pool->shutdown();
				break;

            case SIGHUP:
				break;

            default:
                // handle all other signals
        }
    }

	/**
	 * @return mixed
	 */
	public function run()
	{
		try {
			$this->opts->process();

			$levelMap = array(
				Logger::EMERGENCY,
				Logger::ALERT,
				Logger::CRITICAL,
				Logger::ERROR,
				Logger::WARNING,
				Logger::NOTICE,
				Logger::INFO,
				Logger::DEBUG
			);

			$verbose = $this->opts->getOption("verbose");
			if ($verbose > 0) {
				if ($verbose >= count($levelMap)) {
					$verbose = count($levelMap) - 1;
				}

				$level = $levelMap[$verbose];
				$this->log->pushHandler(new SyslogHandler($this->log->getName(), LOG_DAEMON, $level));
			} else {
				$this->log->pushHandler(new NullHandler(Logger::DEBUG));
			}

			// show help and quit
			$command = $this->opts->getCommand();
			if (! $command || $this->opts->getOption("help")) {
				file_put_contents("php://stdout", PHP_EOL . $this->opts->getHelpText() . PHP_EOL);
				$status = 1;
			} else {
				// call the requested command
				$status = call_user_func($command->getHandler());
			}
		} catch (Exception $e) {
			file_put_contents("php://stderr", PHP_EOL . $e->getMessage() . PHP_EOL . $this->opts->getHelpText() . PHP_EOL);
			$this->log->critical($e->getMessage());
			$status = 127;
		}

		return $status;
	}

	/**
	 * Sets a logger instance on the object.
	 *
	 * @param LoggerInterface $logger
	 */
	public function setLogger(LoggerInterface $logger)
	{
		$this->log = $logger;
	}
}
