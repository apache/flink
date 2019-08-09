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

package org.apache.flink.runtime.jobmaster.slotpool;

/**
 * If a shared slot is over-allocated before it has been resolved,
 * some requests will be rejected with this exception to ensure the
 * total resource requested do not exceed the total resources. The
 * released requests can be retried if couldRetry is marked.
 */
class SharedSlotOversubscribedException extends Exception {

	/** Whether the requester can retry the request. */
	private final boolean canRetry;

	SharedSlotOversubscribedException(String message, boolean canRetry) {
		super(message);
		this.canRetry = canRetry;
	}

	boolean canRetry() {
		return canRetry;
	}
}
