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

package org.apache.flink.mesos.scheduler;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;

/**
 * A builder for the Fenzo task scheduler.
 *
 * <p>Note that the Fenzo-provided {@link TaskScheduler.Builder} cannot be mocked, which motivates this interface.
 */
public interface TaskSchedulerBuilder {

	/**
	 * Set the callback action for rejecting a lease.
	 */
	TaskSchedulerBuilder withLeaseRejectAction(Action1<VirtualMachineLease> action);

	/**
	 * Build a Fenzo task scheduler.
	 */
	TaskScheduler build();
}
