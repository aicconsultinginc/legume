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

interface WorkerInterface extends LoggerAwareInterface
{
    public function __construct();

    /**
     * Allows the worker to collect references determined to be garbage by the
     * optionally given collector.
     *
     * @param callable|null $collector
     * @return int
     */
    public function collect($collector = null);

    /**
     * Start this worker and execute the implemented run method
     *
     * @param mixed $options
     */
    public function start($options = null);

    /**
     * Run loop for this worker process.
     */
    public function run();

    /**
     * Appends the new work to the stack of the referenced worker
     *
     * @param StackableInterface $task
     * @return int
     */
    public function stack(&$task);

    /**
     * Removes the first task (the oldest one) in the stack
     *
     * @return int
     */
    public function unstack();

    /**
     * Returns the number of tasks left on the stack
     *
     * @return int
     */
    public function getStacked();

    /**
     * Whether the worker has been shutdown or not
     *
     * @return bool
     */
    public function isShutdown();

    /**
     * Tell if a Worker is executing a Stackable
     *
     * @return bool
     */
    public function isWorking();

    /**
     * Shuts down the Worker after executing all of the stacked tasks
     *
     * @return bool
     */
    public function shutdown();

    /**
     * Causes the calling context to wait for the referenced Thread to finish executing
     * @return bool
     */
    public function isJoined();

    /**
     * Causes the calling context to wait for the referenced Thread to finish executing
     *
     * @return bool
     */
    public function join();

    /**
     * Tell if the referenced object is executing
     *
     * @return bool
     */
    public function isRunning();
}
