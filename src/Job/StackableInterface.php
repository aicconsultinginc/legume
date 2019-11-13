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

use Exception;
use Psr\Log\LoggerAwareInterface;

interface StackableInterface extends LoggerAwareInterface
{
    public function __construct(callable $callable, $id, $payload);

    /**
     * Returns the Job ID for this stackable.
     *
     * @return string
     */
    public function getId();

    /**
     * Returns the Job Data associated with this stackable.
     *
     * @return string
     */
    public function getPayload();

    /**
     * Run the Stackable callable with job id and data arguments.
     */
    public function run();

    /**
     * Determine whether this Stackable has completed.
     *
     * @return boolean
     */
    public function isComplete();

    /**
     * Determine whether this Stackable encountered an error.
     *
     * @return boolean
     */
    public function isTerminated();
}
