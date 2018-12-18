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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The type Operator edge descriptor.
 */
public class OperatorEdgeDescriptor implements Serializable {

	private final OperatorID sourceOperator;

	private final OperatorID targetOperator;

	private final int typeNumber;

	private final String partitionerDescriptor;

	/**
	 * Instantiates a new Operator edge descriptor.
	 *
	 * @param sourceOperator        the source operator
	 * @param targetOperator        the target operator
	 * @param typeNumber            the type number
	 * @param partitionerDescriptor the partitioner descriptor
	 */
	public OperatorEdgeDescriptor(
		OperatorID sourceOperator,
		OperatorID targetOperator,
		int typeNumber,
		String partitionerDescriptor) {

		this.sourceOperator = checkNotNull(sourceOperator);
		this.targetOperator = checkNotNull(targetOperator);
		this.typeNumber = typeNumber;
		this.partitionerDescriptor = checkNotNull(partitionerDescriptor);
	}

	/**
	 * Gets source operator.
	 *
	 * @return the source operator
	 */
	public OperatorID getSourceOperator() {
		return sourceOperator;
	}

	/**
	 * Gets target operator.
	 *
	 * @return the target operator
	 */
	public OperatorID getTargetOperator() {
		return targetOperator;
	}

	/**
	 * Gets type number.
	 *
	 * @return the type number
	 */
	public int getTypeNumber() {
		return typeNumber;
	}

	/**
	 * Gets partitioner descriptor.
	 *
	 * @return the partitioner descriptor
	 */
	public String getPartitionerDescriptor() {
		return partitionerDescriptor;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).
			append("sourceOperator", sourceOperator).
			append("targetOperator", targetOperator).
			append("typeNumber", typeNumber).
			append("partitionerDescriptor", partitionerDescriptor).
			toString();
	}
}
