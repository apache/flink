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

package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An event to request splits, sent typically from the Source Reader to the Source Enumerator.
 *
 * <p>This event optionally carries the hostname of the location where the reader runs, to support
 * locality-aware work assignment.
 */
public final class RequestSplitEvent implements OperatorEvent {

	private static final long serialVersionUID = 1L;

	@Nullable
	private final String hostName;

	/**
	 * Creates a new {@code RequestSplitEvent} with no host information.
	 */
	public RequestSplitEvent() {
		this(null);
	}

	/**
	 * Creates a new {@code RequestSplitEvent} with a hostname.
	 */
	public RequestSplitEvent(@Nullable String hostName) {
		this.hostName = hostName;
	}

	// ------------------------------------------------------------------------

	@Nullable
	public String hostName() {
		return hostName;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 65932633 + Objects.hashCode(hostName);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final RequestSplitEvent that = (RequestSplitEvent) o;
		return Objects.equals(hostName, that.hostName);
	}

	@Override
	public String toString() {
		return String.format("RequestSplitEvent (host='%s')", hostName);
	}
}
