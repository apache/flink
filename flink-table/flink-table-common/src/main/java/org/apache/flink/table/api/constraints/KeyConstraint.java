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

package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.FieldReferenceExpression;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unique key constraint. It can be declared also as a PRIMARY KEY.
 *
 * @see ConstraintType
 */
@PublicEvolving
public final class KeyConstraint extends AbstractConstraint {
	private final List<FieldReferenceExpression> columns;
	private final ConstraintType type;

	/**
	 * Creates a non enforced {@link ConstraintType#PRIMARY_KEY} constraint. It checks that all
	 * provided columns are of NOT NULL type.
	 */
	public static KeyConstraint primaryKey(String name, List<FieldReferenceExpression> columns) {
		if (columns.stream().anyMatch(c -> c.getOutputDataType().getLogicalType().isNullable())) {
			throw new ValidationException("Cannot define PRIMARY KEY constraint on nullable column.");
		}
		return new KeyConstraint(name, false, ConstraintType.PRIMARY_KEY, columns);
	}

	private KeyConstraint(
			String name,
			boolean enforced,
			ConstraintType type,
			List<FieldReferenceExpression> columns) {
		super(name, enforced);

		this.columns = checkNotNull(columns);
		this.type = checkNotNull(type);
	}

	/**
	 * List of column references for which the primary key was defined.
	 */
	public List<FieldReferenceExpression> getColumns() {
		return columns;
	}

	@Override
	public ConstraintType getType() {
		return type;
	}

	@Override
	String constraintDefinition() {
		return columns.stream().map(FieldReferenceExpression::getName).collect(Collectors.joining(", "));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		KeyConstraint that = (KeyConstraint) o;
		return Objects.equals(columns, that.columns) &&
			type == that.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), columns, type);
	}
}
