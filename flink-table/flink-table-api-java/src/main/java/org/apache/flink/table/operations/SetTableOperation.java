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
import org.apache.flink.table.api.TableSchema;

import java.util.Arrays;
import java.util.List;

/**
 * A set operation on two relations. It provides a way to union, intersect or subtract underlying
 * data sets/streams. Both relations must have equal schemas.
 */
@Internal
public class SetTableOperation implements TableOperation {

	private final TableOperation leftOperation;
	private final TableOperation rightOperation;

	private final SetTableOperationType type;
	private final boolean all;

	public SetTableOperation(
			TableOperation leftOperation,
			TableOperation rightOperation,
			SetTableOperationType type,
			boolean all) {
		this.leftOperation = leftOperation;
		this.rightOperation = rightOperation;
		this.type = type;
		this.all = all;
	}

	/**
	 * Represent kind of this set operation.
	 * <ul>
	 *     <li><b>MINUS</b> returns records from the left relation that do not exist in the right relation</li>
	 *     <li><b>INTERSECT</b> returns records that exist in both relation</li>
	 *     <li><b>UNION</b> returns records from both relations as a single relation</li>
	 * </ul>
	 */
	public enum SetTableOperationType {
		INTERSECT,
		MINUS,
		UNION
	}

	@Override
	public TableSchema getTableSchema() {
		return leftOperation.getTableSchema();
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitSetOperation(this);
	}

	@Override
	public List<TableOperation> getChildren() {
		return Arrays.asList(leftOperation, rightOperation);
	}

	public SetTableOperationType getType() {
		return type;
	}

	public boolean isAll() {
		return all;
	}
}
