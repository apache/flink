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

package org.apache.flink.runtime.iterative.concurrent;

/**
 * Broker to hand over {@link SolutionSetUpdateBarrier} from 
 * {@link org.apache.flink.runtime.iterative.task.IterationHeadPactTask} to
 * {@link org.apache.flink.runtime.iterative.task.IterationTailPactTask}.
 */
public class SolutionSetUpdateBarrierBroker extends Broker<SolutionSetUpdateBarrier> {

	/**
	 * Singleton instance
	 */
	private static final SolutionSetUpdateBarrierBroker INSTANCE = new SolutionSetUpdateBarrierBroker();

	private SolutionSetUpdateBarrierBroker() {
	}

	/**
	 * @return singleton instance
	 */
	public static Broker<SolutionSetUpdateBarrier> instance() {
		return INSTANCE;
	}
}
