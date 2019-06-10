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

import java.util.Objects;
import java.util.Optional;

/**
 * Logical type of a user-defined representation for one or more built-in types. A user-defined
 * type is either a distinct type or a structured type.
 *
 * <p>A {@link UserDefinedType} instance is the result of a catalog lookup or an explicit definition.
 * Therefore, the serialized string representation is a unique {@link ObjectIdentifier}.
 *
 * <p>NOTE: Compared to the SQL standard, this class and subclasses are incomplete. We might add new
 * features such as method declarations in the future.
 *
 * @see DistinctType
 * @see StructuredType
 */
@PublicEvolving
public abstract class UserDefinedType extends LogicalType {

	private final ObjectIdentifier objectIdentifier;

	private final boolean isFinal;

	private final @Nullable String description;

	UserDefinedType(
			boolean isNullable,
			LogicalTypeRoot typeRoot,
			ObjectIdentifier objectIdentifier,
			boolean isFinal,
			@Nullable String description) {
		super(isNullable, typeRoot);
		this.objectIdentifier = Preconditions.checkNotNull(
			objectIdentifier,
			"Object identifier must not be null.");
		this.isFinal = isFinal;
		this.description = description;
	}

	public ObjectIdentifier getObjectIdentifier() {
		return objectIdentifier;
	}

	public boolean isFinal() {
		return isFinal;
	}

	public Optional<String> getDescription() {
		return Optional.ofNullable(description);
	}

	@Override
	public String asSerializableString() {
		return withNullability(objectIdentifier.asSerializableString());
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
		UserDefinedType that = (UserDefinedType) o;
		return isFinal == that.isFinal &&
			objectIdentifier.equals(that.objectIdentifier) &&
			Objects.equals(description, that.description);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), objectIdentifier, isFinal, description);
	}
}
