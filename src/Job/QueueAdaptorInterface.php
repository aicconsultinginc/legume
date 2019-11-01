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

interface QueueAdaptorInterface extends LoggerAwareInterface
{
	/**
	 * @param string $name
	 * @param callable|string $callback
	 */
	public function register($name, $callback);

	/**
	 * @param string $name
	 */
	public function unregister($name);

    /**
     * @param int|null $timeout
     *
     * @return Stackable|null
     */
    public function listen($timeout = null);

    /**
     * @param Stackable $work
     */
    public function touch(Stackable $work);

    /**
     * @param Stackable $work
     */
    public function complete(Stackable $work);

    /**
     * @param Stackable $work
     */
    public function retry(Stackable $work);
}
