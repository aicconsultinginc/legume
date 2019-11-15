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

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Worker;

class ThreadWorker extends Worker
{
    /** @var int $startTime */
    protected $startTime;

    /** @var LoggerInterface */
    private $logger;

    /**
     * @inheritDoc
     */
    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    /**
     * @inheritDoc
     */
    public function run()
    {
        $this->startTime = time();
    }

    /**
     * Use the inherit ALL option by default.
     *
     * @inheritDoc
     */
    public function start($options = PTHREADS_INHERIT_ALL)
    {
        $this->logger->debug("Starting thread worker", array($this->getThreadId(), dechex($options)));
        return parent::start($options);
    }

    /**
     * @inheritDoc
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
