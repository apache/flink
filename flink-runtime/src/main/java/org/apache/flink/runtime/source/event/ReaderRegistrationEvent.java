/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * An {@link OperatorEvent} that registers a {@link org.apache.flink.api.connector.source.SourceReader SourceReader}
 * to the SourceCoordinator.
 */
public class ReaderRegistrationEvent implements OperatorEvent {

	private static final long serialVersionUID = 1L;

	private final int subtaskId;
	private final String location;

	public ReaderRegistrationEvent(int subtaskId, String location) {
		this.subtaskId = subtaskId;
		this.location = location;
	}

	public int subtaskId() {
		return subtaskId;
	}

	public String location() {
		return location;
	}

	@Override
	public String toString() {
		return String.format("ReaderRegistrationEvent[subtaskId = %d, location = %s)", subtaskId, location);
	}
}
