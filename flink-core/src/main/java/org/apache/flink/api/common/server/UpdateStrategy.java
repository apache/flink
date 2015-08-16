/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.api.common.server;

/**
 * Describes the strategy by which the updates to Parameters must be made.
 */

public enum UpdateStrategy{
	/**
	 * BATCH strategy implies that all clients must send their updates to the server for any of the
	 * updates to be applied to the parameter. This means all clients will remain blocked by the
	 * slowest task.
	 */
	BATCH,
	/**
	 * ASYNC strategy means as soon as the server receives an update from a client, it is applied
	 * and the parameter value is updated. This is non-blocking and every client performs updates
	 * independently.
	 */
	ASYNC,
	/**
	 * This is a Stale asynchronous update strategy whereby if the new update is too fresh, meaning
	 * an earlier clocked update is pending, then the new update is blocked till the lower clock
	 * has all updates finished. The gap is defined by a slack value.
	 */
	SSP
}
