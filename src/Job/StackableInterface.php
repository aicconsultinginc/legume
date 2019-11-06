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

use Psr\Container\ContainerInterface as DI;
use Psr\Log\LoggerAwareInterface;

interface StackableInterface extends LoggerAwareInterface
{
	public function __construct(callable $callable, $id, $workload);

	/**
	 * @return string
	 */
	public function getId();

	/**
	 * @return string
	 */
	public function getData();


	public function run();

	/**
	 * Determine whether this Threaded has completed.
	 *
	 * @return boolean
	 */
	public function isComplete();

	public function isTerminated();

	public function isGarbage();

	public function setGarbage();
}
