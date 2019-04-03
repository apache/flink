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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a field in an input. The reference contains:
 * <ul>
 *     <li>type</li>
 *     <li>index of an input the field belongs to</li>
 *     <li>index of a field within the corresponding input</li>
 * </ul>
 */
@PublicEvolving
public final class FieldReferenceExpression implements Expression {

	private final String name;

	private final TypeInformation<?> resultType;

	private final int inputIndex;

	private final int fieldIndex;

	public FieldReferenceExpression(
			String name,
			TypeInformation<?> resultType,
			int inputIndex,
			int fieldIndex) {
		Preconditions.checkArgument(inputIndex >= 0, "Index of input should be a positive number");
		Preconditions.checkArgument(fieldIndex >= 0, "Index of field should be a positive number");
		this.name = Preconditions.checkNotNull(name);
		this.resultType = Preconditions.checkNotNull(resultType);
		this.inputIndex = inputIndex;
		this.fieldIndex = fieldIndex;
	}

	public String getName() {
		return name;
	}

	public TypeInformation<?> getResultType() {
		return resultType;
	}

	public int getInputIndex() {
		return inputIndex;
	}

	public int getFieldIndex() {
		return fieldIndex;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitFieldReference(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FieldReferenceExpression that = (FieldReferenceExpression) o;
		return Objects.equals(name, that.name) &&
			Objects.equals(resultType, that.resultType) &&
			inputIndex == that.inputIndex &&
			fieldIndex == that.fieldIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, resultType, inputIndex, fieldIndex);
	}

	@Override
	public String toString() {
		return name;
	}
}
