/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.Preconditions;

/**
 * This class generates a string that can be used to identify an operator subtask.
 */
public class OperatorSubtaskDescriptionText {

	/** Cached description result. */
	private final String description;

	public OperatorSubtaskDescriptionText(OperatorID operatorId, String operatorClass, int subtaskIndex, int numberOfTasks) {

		Preconditions.checkArgument(numberOfTasks > 0);
		Preconditions.checkArgument(subtaskIndex >= 0);
		Preconditions.checkArgument(subtaskIndex < numberOfTasks);

		this.description = operatorClass +
				"_" + operatorId +
				"_(" + (1 + subtaskIndex) + "/" + numberOfTasks + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		OperatorSubtaskDescriptionText that = (OperatorSubtaskDescriptionText) o;

		return description.equals(that.description);
	}

	@Override
	public int hashCode() {
		return description.hashCode();
	}

	@Override
	public String toString() {
		return description;
	}
}
