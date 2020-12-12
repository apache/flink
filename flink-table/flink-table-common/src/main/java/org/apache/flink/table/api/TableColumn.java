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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Representation of a table column in the API.
 *
 * <p>A table column is fully resolved with a name and {@link DataType}. It describes either a
 * {@link PhysicalColumn}, {@link ComputedColumn}, or {@link MetadataColumn}.
 */
@PublicEvolving
public abstract class TableColumn {

	private final String name;

	private final DataType type;

	private TableColumn(String name, DataType type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * Creates a regular table column that represents physical data.
	 */
	public static PhysicalColumn physical(String name, DataType type) {
		Preconditions.checkNotNull(name, "Column name can not be null.");
		Preconditions.checkNotNull(type, "Column type can not be null.");
		return new PhysicalColumn(name, type);
	}

	/**
	 * Creates a computed column that is computed from the given SQL expression.
	 */
	public static ComputedColumn computed(String name, DataType type, String expression) {
		Preconditions.checkNotNull(name, "Column name can not be null.");
		Preconditions.checkNotNull(type, "Column type can not be null.");
		Preconditions.checkNotNull(expression, "Column expression can not be null.");
		return new ComputedColumn(name, type, expression);
	}

	/**
	 * Creates a metadata column from metadata of the given column name.
	 *
	 * <p>The column is not virtual by default.
	 */
	public static MetadataColumn metadata(String name, DataType type) {
		return metadata(name, type, null, false);
	}

	/**
	 * Creates a metadata column from metadata of the given column name.
	 *
	 * <p>Allows to specify whether the column is virtual or not.
	 */
	public static MetadataColumn metadata(String name, DataType type, boolean isVirtual) {
		return metadata(name, type, null, isVirtual);
	}

	/**
	 * Creates a metadata column from metadata of the given alias.
	 *
	 * <p>The column is not virtual by default.
	 */
	public static MetadataColumn metadata(String name, DataType type, String metadataAlias) {
		Preconditions.checkNotNull(metadataAlias, "Metadata alias can not be null.");
		return metadata(name, type, metadataAlias, false);
	}

	/**
	 * Creates a metadata column from metadata of the given column name or from metadata of the given
	 * alias (if not null).
	 *
	 * <p>Allows to specify whether the column is virtual or not.
	 */
	public static MetadataColumn metadata(
			String name,
			DataType type,
			@Nullable String metadataAlias,
			boolean isVirtual) {
		Preconditions.checkNotNull(name, "Column name can not be null.");
		Preconditions.checkNotNull(type, "Column type can not be null.");
		return new MetadataColumn(name, type, metadataAlias, isVirtual);
	}

	/**
	 * Returns whether the given column is a physical column of a table; neither computed
	 * nor metadata.
	 */
	public abstract boolean isPhysical();

	/**
	 * Returns whether the given column is persisted in a sink operation.
	 */
	public abstract boolean isPersisted();

	/** Returns the data type of this column. */
	public DataType getType() {
		return this.type;
	}

	/** Returns the name of this column. */
	public String getName() {
		return name;
	}

	/**
	 * Returns a string that summarizes this column for printing to a console.
	 */
	public String asSummaryString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(": ");
		sb.append(type);
		explainExtras().ifPresent(e -> {
			sb.append(" ");
			sb.append(e);
		});
		return sb.toString();
	}

	/**
	 * Returns an explanation of specific column extras next to name and type.
	 */
	public abstract Optional<String> explainExtras();

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableColumn that = (TableColumn) o;
		return Objects.equals(this.name, that.name)
			&& Objects.equals(this.type, that.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.name, this.type);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}

	// --------------------------------------------------------------------------------------------
	// Specific kinds of columns
	// --------------------------------------------------------------------------------------------

	/**
	 * Representation of a physical column.
	 */
	public static class PhysicalColumn extends TableColumn {

		private PhysicalColumn(String name, DataType type) {
			super(name, type);
		}

		@Override
		public boolean isPhysical() {
			return true;
		}

		@Override
		public boolean isPersisted() {
			return true;
		}

		@Override
		public Optional<String> explainExtras() {
			return Optional.empty();
		}
	}

	/**
	 * Representation of a computed column.
	 */
	public static class ComputedColumn extends TableColumn {

		private final String expression;

		private ComputedColumn(String name, DataType type, String expression) {
			super(name, type);
			this.expression = expression;
		}

		@Override
		public boolean isPhysical() {
			return false;
		}

		@Override
		public boolean isPersisted() {
			return false;
		}

		public String getExpression() {
			return expression;
		}

		@Override
		public Optional<String> explainExtras() {
			return Optional.of("AS " + expression);
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
			ComputedColumn that = (ComputedColumn) o;
			return expression.equals(that.expression);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), expression);
		}
	}

	/**
	 * Representation of a metadata column.
	 */
	public static class MetadataColumn extends TableColumn {

		private final @Nullable String metadataAlias;

		private final boolean isVirtual;

		private MetadataColumn(
				String name,
				DataType type,
				@Nullable String metadataAlias,
				boolean isVirtual) {
			super(name, type);
			this.metadataAlias = metadataAlias;
			this.isVirtual = isVirtual;
		}

		public boolean isVirtual() {
			return isVirtual;
		}

		public Optional<String> getMetadataAlias() {
			return Optional.ofNullable(metadataAlias);
		}

		@Override
		public boolean isPhysical() {
			return false;
		}

		@Override
		public boolean isPersisted() {
			return !isVirtual;
		}

		@Override
		public Optional<String> explainExtras() {
			final StringBuilder sb = new StringBuilder();
			sb.append("METADATA");
			if (metadataAlias != null) {
				sb.append(" FROM ");
				sb.append("'");
				sb.append(metadataAlias);
				sb.append("'");
			}
			if (isVirtual) {
				sb.append(" VIRTUAL");
			}
			return Optional.of(sb.toString());
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
			MetadataColumn that = (MetadataColumn) o;
			return isVirtual == that.isVirtual &&
				Objects.equals(metadataAlias, that.metadataAlias);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), metadataAlias, isVirtual);
		}
	}
}
