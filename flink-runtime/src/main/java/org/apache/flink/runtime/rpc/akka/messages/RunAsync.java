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

package org.apache.flink.runtime.rpc.akka.messages;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Message for asynchronous runnable invocations
 */
public final class RunAsync implements Serializable {
	private static final long serialVersionUID = -3080595100695371036L;

	/** The runnable to be executed. Transient, so it gets lost upon serialization */ 
	private final transient Runnable runnable;

	/** The delay after which the runnable should be called */
	private final long delay;

	/**
	 * 
	 * @param runnable  The Runnable to run.
	 * @param delay     The delay in milliseconds. Zero indicates immediate execution.
	 */
	public RunAsync(Runnable runnable, long delay) {
		checkArgument(delay >= 0);
		this.runnable = checkNotNull(runnable);
		this.delay = delay;
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public long getDelay() {
		return delay;
	}
}
