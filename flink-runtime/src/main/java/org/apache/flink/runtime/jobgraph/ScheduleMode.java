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

package org.apache.flink.runtime.jobgraph;

/**
 * The ScheduleMode decides whether vertices should be scheduled with
 * {@link org.apache.flink.runtime.schedule.EagerSchedulingPlugin} or
 * {@link org.apache.flink.runtime.schedule.StepwiseSchedulingPlugin}.
 */
public enum ScheduleMode {

	/** Schedule vertices lazily from the sources. {@link org.apache.flink.runtime.schedule.StepwiseSchedulingPlugin}
	 * will be used in this mode.
	 */
	LAZY_FROM_SOURCES,

	/** Schedules all vertices immediately. {@link org.apache.flink.runtime.schedule.EagerSchedulingPlugin}
	 * will be used in this mode.
	 */
	EAGER
}
