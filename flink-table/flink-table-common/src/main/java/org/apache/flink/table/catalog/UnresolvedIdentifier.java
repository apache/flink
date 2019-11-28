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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Identifier of an object, such as table, view, function or type in a catalog. This identifier
 * cannot be used directly to access an object in a catalog manager, but has to be first
 * fully resolved into {@link ObjectIdentifier}.
 */
@Internal
public final class UnresolvedIdentifier {

	private final @Nullable String catalogName;

	private final @Nullable String databaseName;

	private final String objectName;

	/**
	 * Constructs an {@link UnresolvedIdentifier} from an array of identifier segments.
	 * The length of the path must be between 1 (only object name) and 3 (fully qualified
	 * identifier with catalog, database and object name).
	 *
	 * @param path array of identifier segments
	 * @return an identifier that must be resolved before accessing an object from a catalog manager
	 */
	public static UnresolvedIdentifier of(String... path) {
		if (path == null) {
			throw new ValidationException("Object identifier can not be null!");
		}
		if (path.length < 1 || path.length > 3) {
			throw new ValidationException("Object identifier must consist of 1 to 3 parts.");
		}
		if (Arrays.stream(path).anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
			throw new ValidationException("Parts of the object identifier are null or whitespace-only.");
		}

		if (path.length == 3) {
			return new UnresolvedIdentifier(path[0], path[1], path[2]);
		} else if (path.length == 2) {
			return new UnresolvedIdentifier(null, path[0], path[1]);
		} else {
			return new UnresolvedIdentifier(null, null, path[0]);
		}
	}

	private UnresolvedIdentifier(
			@Nullable String catalogName,
			@Nullable String databaseName,
			String objectName) {
		this.catalogName = catalogName;
		this.databaseName = databaseName;
		this.objectName = Preconditions.checkNotNull(objectName, "Object name must not be null.");
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
	 * Returns a string that summarizes this instance for printing to a console or log.
	 */
	public String asSummaryString() {
		return Stream.of(catalogName, databaseName, objectName)
			.filter(Objects::nonNull)
			.map(EncodingUtils::escapeIdentifier)
			.collect(Collectors.joining("."));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		UnresolvedIdentifier that = (UnresolvedIdentifier) o;
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
		return asSummaryString();
	}
}
