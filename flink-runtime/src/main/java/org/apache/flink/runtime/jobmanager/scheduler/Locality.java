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

package org.apache.flink.runtime.jobmanager.scheduler;

public enum Locality {

	/** No constraint existed on the task placement. */
	UNCONSTRAINED,

	/** The task was scheduled into the same TaskManager as requested */
	LOCAL,

	/** The task was scheduled onto the same host as requested */
	HOST_LOCAL,

	/** The task was scheduled to a destination not included in its locality preferences. */
	NON_LOCAL,

	/** No locality information was provided, it is unknown if the locality was respected */
	UNKNOWN
}
