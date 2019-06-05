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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical type of a user-defined distinct type. A distinct type specifies an identifier and is backed
 * by a source type. A distinct type has the same internal representation as a source type, but is
 * considered to be a separate and incompatible data type for most operations. Compared to the SQL
 * standard, every non-user-defined type can be used as a source type.
 *
 * <p>A distinct type can always be cast to its source type and vice versa.
 *
 * <p>Distinct types are implicitly final and do not support super types.
 *
 * <p>Most other properties are forwarded from the source type. Thus, ordering and comparision among
 * the same distinct types are supported.
 *
 * <p>The serialized string representation is the fully qualified name of this type which means that
 * the type must have been registered in a catalog.
 */
@PublicEvolving
public final class DistinctType extends UserDefinedType {

	/**
	 * A builder for a {@link DistinctType}. Intended for future extensibility.
	 */
	public static final class Builder {

		private final ObjectIdentifier objectIdentifier;

		private final LogicalType sourceType;

		private @Nullable String description;

		public Builder(ObjectIdentifier objectIdentifier, LogicalType sourceType) {
			this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier, "Object identifier must not be null.");
			this.sourceType = Preconditions.checkNotNull(sourceType, "Source type must not be null.");

			Preconditions.checkArgument(
				!sourceType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.USER_DEFINED),
				"Source type must not be a user-defined type.");
		}

		public Builder setDescription(String description) {
			this.description = Preconditions.checkNotNull(description, "Description must not be null");
			return this;
		}

		public DistinctType build() {
			return new DistinctType(objectIdentifier, sourceType, description);
		}
	}

	private final LogicalType sourceType;

	private DistinctType(
			ObjectIdentifier objectIdentifier,
			LogicalType sourceType,
			@Nullable String description) {
		super(
			sourceType.isNullable(),
			LogicalTypeRoot.DISTINCT_TYPE,
			objectIdentifier,
			true,
			description);
		this.sourceType = Preconditions.checkNotNull(sourceType, "Source type must not be null.");
	}

	public LogicalType getSourceType() {
		return sourceType;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new DistinctType(
			getObjectIdentifier(),
			sourceType.copy(isNullable),
			getDescription().orElse(null));
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return sourceType.supportsInputConversion(clazz);
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		return sourceType.supportsOutputConversion(clazz);
	}

	@Override
	public Class<?> getDefaultConversion() {
		return sourceType.getDefaultConversion();
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.singletonList(sourceType);
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
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
		DistinctType that = (DistinctType) o;
		return sourceType.equals(that.sourceType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), sourceType);
	}
}
