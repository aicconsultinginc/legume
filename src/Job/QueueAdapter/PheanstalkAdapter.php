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
namespace Legume\Job\QueueAdapter;

use Legume\Job\HandlerInterface;
use Legume\Job\QueueAdaptorInterface;
use Legume\Job\Stackable\ForkStackable;
use Legume\Job\Stackable\ThreadStackable;
use Legume\Job\StackableInterface;
use Pheanstalk\Job;
use Pheanstalk\Pheanstalk;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class PheanstalkAdapter implements QueueAdaptorInterface
{
    /** @var Pheanstalk $client */
    protected $client;

	/** @var callable[string] $jobs */
	protected $jobs;

    /** @var LoggerInterface $logger */
    protected $logger;

	/**
	 * @param Pheanstalk $client
	 */
    public function __construct(Pheanstalk $client)
    {
        $this->client = $client;
        $this->jobs = array();
		$this->logger = new NullLogger();
    }

    /**
     * Make sure we close the connection to beanstalk
     */
    public function __destruct()
    {
        if (isset($this->client)) {
            $this->client->getConnection()->disconnect();
        }
    }

	/**
	 * @inheritdoc
	 */
    public function register($name, $callback)
    {
		$this->jobs[$name] = $callback;
        $this->client->watch($name);
    }

	/**
	 * @inheritdoc
	 */
    public function unregister($name)
    {
        $this->client->ignore($name);
        unset($this->jobs[$name]);
    }

	/**
	 * @inheritdoc
	 */
    public function listen($timeout = null)
    {
        $stackable = null;

        $job = $this->client->reserve($timeout);
        if ($job !== false) {
            $info = $this->client->statsJob($job);
            $tube = $info["tube"];

            if (isset($this->jobs[$tube])) {
            	$callable = $this->jobs[$tube];

            	if (is_string($callable) && in_array(HandlerInterface::class, class_implements($callable, true))) {
					/** @var HandlerInterface $callable */
            		$callable = new $callable();
					$callable->setLogger($this->logger);
				}

				if (is_callable($callable)) {
            		$stackable = new ForkStackable(
						$callable,
						$job->getId(),
						$job->getData()
					);
				} else {
            		$this->logger->warning("Failed to locate callable for job '{$tube}'!");
				}
            } else {
				$this->logger->warning("No job registered for '{$tube}'!");
			}
        }

        return $stackable;
    }

    /**
     * @inheritdoc
     */
    public function complete(StackableInterface $work)
    {
        $job = new Job($work->getId(), $work->getPayload());

        $this->client->delete($job);
    }

    /**
     * @inheritdoc
     */
    public function retry(StackableInterface $work)
    {
        $job = new Job($work->getId(), $work->getPayload());

        $this->client->release($job);
    }

	/**
	 * @inheritdoc
	 */
	public function touch(StackableInterface $work)
	{
		$job = new Job($work->getId(), $work->getPayload());

		$this->client->touch($job);
	}

	/**
	 * @inheritdoc
	 */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
