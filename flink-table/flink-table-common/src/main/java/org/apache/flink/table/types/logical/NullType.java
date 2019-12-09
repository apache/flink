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

import java.util.Collections;
import java.util.List;

/**
 * Logical type for representing untyped {@code NULL} values. The null type is an extension to the
 * SQL standard. A null type has no other value except {@code NULL}, thus, it can be cast to any
 * nullable type similar to JVM semantics.
 *
 * <p>This type helps in representing unknown types in API calls that use a {@code NULL} literal as
 * well as bridging to formats such as JSON or Avro that define such a type as well.
 *
 * <p>The serialized string representation is {@code NULL}.
 */
@PublicEvolving
public final class NullType extends LogicalType {

	private static final String FORMAT = "NULL";

	private static final Class<?> INPUT_CONVERSION = Object.class;

	private static final Class<?> DEFAULT_CONVERSION = Object.class;

	public NullType() {
		super(true, LogicalTypeRoot.NULL);
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		if (!isNullable) {
			throw new TableException(
				"The nullability of a NULL type cannot be disabled because the type must always " +
					"be able to contain a null value.");
		}
		return new NullType();
	}

	@Override
	public String asSerializableString() {
		return FORMAT;
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return INPUT_CONVERSION.equals(clazz);
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		// any nullable class is supported
		return !clazz.isPrimitive();
	}

	@Override
	public Class<?> getDefaultConversion() {
		return DEFAULT_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
