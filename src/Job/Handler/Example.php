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
namespace Legume\Job\Handler;

use Legume\Job\HandlerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class Example implements HandlerInterface
{
    // Setup an optional time limit for this job.
    const TIMEOUT = 120;

    /** @var LoggerInterface $log */
    protected $log;

    public function __construct()
	{
		$this->log = new NullLogger();
	}

	/**
     * @param string $jobId
     * @param string $workload
     *
     * @return int
     */
    public function __invoke($jobId, $workload)
    {
        $this->log->info("{$jobId}: Processing example job for {$workload} seconds...");

        $status = sleep($workload);
        if ($status === false) {
            throw new RuntimeException("Sleep failed");
        }
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
