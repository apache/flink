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

import java.util.Objects;

/**
 * An ID for physical instance of the operator.
 */
public class OperatorInstanceID  {

	private final int subtaskId;
	private final OperatorID operatorId;

	public static OperatorInstanceID of(int subtaskId, OperatorID operatorID) {
		return new OperatorInstanceID(subtaskId, operatorID);
	}

	public OperatorInstanceID(int subtaskId, OperatorID operatorId) {
		this.subtaskId = subtaskId;
		this.operatorId = operatorId;
	}

	public int getSubtaskId() {
		return subtaskId;
	}

	public OperatorID getOperatorId() {
		return operatorId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskId, operatorId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof OperatorInstanceID)) {
			return false;
		}
		OperatorInstanceID other = (OperatorInstanceID) obj;
		return this.subtaskId == other.subtaskId &&
			Objects.equals(this.operatorId, other.operatorId);
	}

	@Override
	public String toString() {
		return String.format("<%d, %s>", subtaskId, operatorId);
	}
}
