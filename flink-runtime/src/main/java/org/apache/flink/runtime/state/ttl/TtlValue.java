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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * This class wraps user value of state with TTL.
 *
 * @param <T> Type of the user value of state with TTL
 */
class TtlValue<T> implements Serializable {
	private static final long serialVersionUID = 5221129704201125020L;

	private final T userValue;
	private final long lastAccessTimestamp;

	TtlValue(T userValue, long lastAccessTimestamp) {
		Preconditions.checkNotNull(userValue);
		this.userValue = userValue;
		this.lastAccessTimestamp = lastAccessTimestamp;
	}

	T getUserValue() {
		return userValue;
	}

	long getLastAccessTimestamp() {
		return lastAccessTimestamp;
	}
}
