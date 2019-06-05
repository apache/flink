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

package org.apache.flink.table.catalog;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;

/**
 * Identifies an object in a catalog. It allows to identify objects such as tables, views, function,
 * or types in a catalog. An identifier must not be fully qualified. It is the responsibility of the
 * catalog manager to resolve an identifier to an object; e.g. by assuming default catalog/database.
 *
 * <p>While {@link ObjectPath} is used within the same catalog, instances of this class can be used
 * across catalogs.
 *
 * <p>Two objects are considered equal if they share the same type identifier in a stable session context.
 */
public final class ObjectIdentifier implements Serializable {

	private @Nullable String catalogName;

	private @Nullable String databaseName;

	private String objectName;

	public static ObjectIdentifier of(String catalogName, String databaseName, String objectName) {
		return new ObjectIdentifier(
			Preconditions.checkNotNull(catalogName, "Catalog name must not be null."),
			Preconditions.checkNotNull(databaseName, "Database name must not be null."),
			Preconditions.checkNotNull(objectName, "Object name must not be null."));
	}

	public static ObjectIdentifier of(String databaseName, String objectName) {
		return new ObjectIdentifier(
			null,
			Preconditions.checkNotNull(databaseName, "Database name must not be null."),
			Preconditions.checkNotNull(objectName, "Object name must not be null."));
	}

	public static ObjectIdentifier of(String objectName) {
		return new ObjectIdentifier(
			null,
			null,
			Preconditions.checkNotNull(objectName, "Object name must not be null."));
	}

	private ObjectIdentifier(
			@Nullable String catalogName,
			@Nullable String databaseName,
			String objectName) {
		this.catalogName = catalogName;
		this.databaseName = databaseName;
		this.objectName = objectName;
	}

	public Optional<String> getCatalogName() {
		return Optional.ofNullable(catalogName);
	}

	public Optional<String> getDatabaseName() {
		return Optional.ofNullable(databaseName);
	}

	public String getObjectName() {
		return objectName;
	}

	/**
	 * Returns a string that fully serializes this instance. The serialized string can be used for
	 * transmitting or persisting an object identifier.
	 */
	public String asSerializableString() {
		final StringBuilder sb = new StringBuilder();
		if (catalogName != null) {
			sb.append(escapeIdentifier(catalogName));
			sb.append('.');
		}
		if (databaseName != null) {
			sb.append(escapeIdentifier(databaseName));
			sb.append('.');
		}
		sb.append(escapeIdentifier(objectName));
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ObjectIdentifier that = (ObjectIdentifier) o;
		return Objects.equals(catalogName, that.catalogName) &&
			Objects.equals(databaseName, that.databaseName) &&
			objectName.equals(that.objectName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(catalogName, databaseName, objectName);
	}

	@Override
	public String toString() {
		return asSerializableString();
	}
}
