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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.SetTableOperation.SetTableOperationType;

import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.flink.table.operations.SetTableOperation.SetTableOperationType.UNION;

/**
 * Utility class for creating a valid {@link SetTableOperation}.
 */
@Internal
public class SetOperationFactory {

	private final boolean isStreaming;

	public SetOperationFactory(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid algebraic operation.
	 *
	 * @param type type of operation to create
	 * @param left first relational operation of the operation
	 * @param right second relational operation of the operation
	 * @param all flag defining how duplicates should be handled
	 * @return creates a valid algebraic operation
	 */
	public TableOperation create(
			SetTableOperationType type,
			TableOperation left,
			TableOperation right,
			boolean all) {
		failIfStreaming(type, all);
		validateSetOperation(type, left, right);
		return new SetTableOperation(left, right, type, all);
	}

	private void validateSetOperation(
			SetTableOperationType operationType,
			TableOperation left,
			TableOperation right) {
		TableSchema leftSchema = left.getTableSchema();
		int leftFieldCount = leftSchema.getFieldCount();
		TableSchema rightSchema = right.getTableSchema();
		int rightFieldCount = rightSchema.getFieldCount();

		if (leftFieldCount != rightFieldCount) {
			throw new ValidationException(
				format(
					"The %s operation on two tables of different column sizes: %d and %d is not supported",
					operationType.toString().toLowerCase(),
					leftFieldCount,
					rightFieldCount));
		}

		TypeInformation<?>[] leftFieldTypes = leftSchema.getFieldTypes();
		TypeInformation<?>[] rightFieldTypes = rightSchema.getFieldTypes();
		boolean sameSchema = IntStream.range(0, leftFieldCount)
			.allMatch(idx -> leftFieldTypes[idx].equals(rightFieldTypes[idx]));

		if (!sameSchema) {
			throw new ValidationException(
				format(
					"The %s operation on two tables of different schemas: %s and %s is not supported.",
					operationType.toString().toLowerCase(),
					leftSchema,
					rightSchema));
		}
	}

	private void failIfStreaming(SetTableOperationType type, boolean all) {
		boolean shouldFailInCaseOfStreaming = !all || type != UNION;

		if (isStreaming && shouldFailInCaseOfStreaming) {
			throw new ValidationException(
				format(
					"The %s operation on two unbounded tables is currently not supported.",
					type));
		}
	}
}
