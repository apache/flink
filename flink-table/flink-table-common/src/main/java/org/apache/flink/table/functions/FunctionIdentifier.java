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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Identifies a system function with function name or a catalog function with a fully qualified identifier.
 * Function catalog is responsible for resolving an identifier to a function.
 */
@PublicEvolving
public final class FunctionIdentifier implements Serializable {

	private final ObjectIdentifier objectIdentifier;

	private final String functionName;

	public static FunctionIdentifier of(ObjectIdentifier oi){
		return new FunctionIdentifier(oi);
	}

	public static FunctionIdentifier of(String functionName){
		return new FunctionIdentifier(functionName);
	}

	private FunctionIdentifier(ObjectIdentifier objectIdentifier){
		checkNotNull(objectIdentifier, "Object identifier cannot be null");
		this.objectIdentifier = normalizeObjectIdentifier(objectIdentifier);
		this.functionName = null;
	}

	private FunctionIdentifier(String functionName){
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(functionName),
		"function name cannot be null or empty string");
		this.functionName = normalizeName(functionName);
		this.objectIdentifier = null;
	}

	/**
	 * Normalize a function name.
	 */
	public static String normalizeName(String name) {
		return name.toLowerCase();
	}

	/**
	 * Normalize an object identifier by only normalizing the function name.
	 */
	public static ObjectIdentifier normalizeObjectIdentifier(ObjectIdentifier oi) {
		return ObjectIdentifier.of(
			oi.getCatalogName(),
			oi.getDatabaseName(),
			normalizeName(oi.getObjectName()));
	}

	public Optional<ObjectIdentifier> getIdentifier(){
		return Optional.ofNullable(objectIdentifier);
	}

	public Optional<String> getSimpleName(){
		return Optional.ofNullable(functionName);
	}

	/**
	 * Returns a string that fully serializes this instance. The serialized string can be used for
	 * transmitting or persisting an object identifier.
	 */
	public String asSerializableString() {
		if (objectIdentifier != null) {
			return String.format(
				"%s.%s.%s",
				escapeIdentifier(objectIdentifier.getCatalogName()),
				escapeIdentifier(objectIdentifier.getDatabaseName()),
				escapeIdentifier(objectIdentifier.getObjectName()));
		} else {
			return String.format("%s", escapeIdentifier(functionName));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionIdentifier that = (FunctionIdentifier) o;

		if (getIdentifier() != null && getIdentifier().equals(that.getIdentifier())) {
			return true;
		} else {
			return functionName.equals(that.functionName);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(objectIdentifier, functionName);
	}

	@Override
	public String toString() {
		return asSerializableString();
	}
}
