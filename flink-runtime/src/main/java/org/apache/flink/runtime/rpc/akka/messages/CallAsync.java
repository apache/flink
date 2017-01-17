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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Message for asynchronous callable invocations
 */
public final class CallAsync implements Serializable {
	private static final long serialVersionUID = 2834204738928484060L;

	private transient Callable<?> callable;

	public CallAsync(Callable<?> callable) {
		this.callable = Preconditions.checkNotNull(callable);
	}

	public Callable<?> getCallable() {
		return callable;
	}
}
