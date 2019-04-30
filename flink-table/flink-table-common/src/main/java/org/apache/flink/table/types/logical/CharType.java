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
import org.apache.flink.table.api.ValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a fixed-length character string.
 *
 * <p>The serialized string representation is {@code CHAR(n)} where {@code n} is the number of
 * code points. {@code n} must have a value between 1 and 255 (both inclusive). If no length is
 * specified, {@code n} is equal to 1.
 *
 * <p>A conversion from and to {@code byte[]} assumes UTF-8 encoding.
 */
@PublicEvolving
public final class CharType extends LogicalType {

	private static final int MIN_LENGTH = 1;

	private static final int MAX_LENGTH = 255;

	private static final int DEFAULT_LENGTH = 1;

	private static final String FORMAT = "CHAR(%d)";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		String.class.getName(),
		byte[].class.getName(),
		"org.apache.flink.table.dataformat.BinaryString");

	private static final Class<?> DEFAULT_CONVERSION = String.class;

	private final int length;

	public CharType(boolean isNullable, int length) {
		super(isNullable, LogicalTypeRoot.CHAR);
		if (length < MIN_LENGTH || length > MAX_LENGTH) {
			throw new ValidationException(
				String.format(
					"Character string length must be between %d and %d (both inclusive).",
					MIN_LENGTH,
					MAX_LENGTH));
		}
		this.length = length;
	}

	public CharType(int length) {
		this(true, length);
	}

	public CharType() {
		this(DEFAULT_LENGTH);
	}

	public int getLength() {
		return length;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new CharType(isNullable, length);
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, length);
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
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
		CharType charType = (CharType) o;
		return length == charType.length;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), length);
	}
}
