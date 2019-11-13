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

namespace Legume\Job;

use Psr\Log\LoggerAwareInterface;

interface ManagerInterface extends LoggerAwareInterface
{
    /**
     * Manager constructor.
     *
     * @param QueueAdaptorInterface $adaptor
     */
    public function __construct(QueueAdaptorInterface $adaptor);

    /**
     * A Callable collector that returns a boolean on whether the task can be collected
     * or not. Only in rare cases should a custom collector need to be used.
     *
     * @param callable|null $collector
     *
     * @return int
     */
    public function collect($collector = null);

    /**
     * Shutdown the Workers in this Pool
     */
    public function shutdown();

    /**
     * Set the maximum number of Workers this Pool can create
     *
     * @param int $size
     */
    public function resize(int $size);

    /**
     * Run loop for this worker process.
     */
    public function run();
}
