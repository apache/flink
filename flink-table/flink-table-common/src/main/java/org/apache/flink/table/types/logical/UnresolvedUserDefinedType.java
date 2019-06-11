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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Placeholder type of an unresolved user-defined type that is identified by a partially or fully
 * qualified path ({@code [catalog].[database].[type]}).
 *
 * <p>It assumes that a type has been registered in a catalog and just needs to be resolved to a
 * {@link DistinctType} or {@link StructuredType}.
 *
 * <p>Two unresolved types are considered equal if they share the same path in a stable session context.
 *
 * @see UserDefinedType
 */
@PublicEvolving
public final class UnresolvedUserDefinedType extends LogicalType {

	private final @Nullable String catalog;

	private final @Nullable String database;

	private final String typeIdentifier;

	public UnresolvedUserDefinedType(
			boolean isNullable,
			@Nullable String catalog,
			@Nullable String database,
			String typeIdentifier) {
		super(isNullable, LogicalTypeRoot.UNRESOLVED);
		this.catalog = catalog;
		this.database = database;
		this.typeIdentifier = Preconditions.checkNotNull(
			typeIdentifier,
			"Type identifier must not be null.");
	}

	public UnresolvedUserDefinedType(
			@Nullable String catalog,
			@Nullable String database,
			String typeIdentifier) {
		this(true, catalog, database, typeIdentifier);
	}

	public Optional<String> getCatalog() {
		return Optional.ofNullable(catalog);
	}

	public Optional<String> getDatabase() {
		return Optional.ofNullable(database);
	}

	public String getTypeIdentifier() {
		return typeIdentifier;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new UnresolvedUserDefinedType(isNullable, catalog, database, typeIdentifier);
	}

	@Override
	public String asSummaryString() {
		final String path = Stream.of(catalog, database, typeIdentifier)
			.filter(Objects::nonNull)
			.map(EncodingUtils::escapeIdentifier)
			.collect(Collectors.joining("."));
		return withNullability(path);
	}

	@Override
	public String asSerializableString() {
		throw new TableException(
			"An unresolved user-defined type has no serializable string representation. It " +
				"needs to be resolved into a proper user-defined type.");
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		throw new TableException("An unresolved user-defined type does not support any input conversion.");
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		throw new TableException("An unresolved user-defined type does not support any output conversion.");
	}

	@Override
	public Class<?> getDefaultConversion() {
		throw new TableException("An unresolved user-defined type has no default conversion.");
	}

	@Override
	public List<LogicalType> getChildren() {
		throw new TableException("An unresolved user-defined type cannot return children.");
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
		UnresolvedUserDefinedType that = (UnresolvedUserDefinedType) o;
		return Objects.equals(catalog, that.catalog) &&
			Objects.equals(database, that.database) &&
			typeIdentifier.equals(that.typeIdentifier);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), catalog, database, typeIdentifier);
	}
}
