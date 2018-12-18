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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Any operation key for the {@link AbstractAsynchronousOperationHandlers} must extend this class.
 * It is used to store the trigger id.
 */
public class OperationKey {

	private final TriggerId triggerId;

	public OperationKey(TriggerId triggerId) {
		this.triggerId = Preconditions.checkNotNull(triggerId);
	}

	/**
	 * Get the trigger id for the given operation key.
	 *
	 * @return trigger id
	 */
	public TriggerId getTriggerId() {
		return triggerId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		OperationKey that = (OperationKey) o;
		return Objects.equals(triggerId, that.triggerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(triggerId);
	}
}
