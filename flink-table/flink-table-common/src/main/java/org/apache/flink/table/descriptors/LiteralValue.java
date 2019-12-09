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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Descriptor for a literal value. A literal value consists of a type and the actual value.
 * Expression values are not allowed.
 *
 * <p>If no type is set, the type is automatically derived from the value. Currently,
 * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
 *
 * <p>Examples:
 * - "true", "false" -> BOOLEAN
 * - "42", "-5" -> INT
 * - "2.0", "1234.222" -> DOUBLE
 * - VARCHAR otherwise
 */
@PublicEvolving
public class LiteralValue extends HierarchyDescriptor {
	private String typeInfo;
	private Object value;

	/**
	 * Type information of the literal value. E.g. Types.BOOLEAN.
	 *
	 * @param typeInfo type information describing the value
	 */
	public LiteralValue of(TypeInformation<?> typeInfo) {
		Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
		this.typeInfo = TypeStringUtils.writeTypeInfo(typeInfo);
		return this;
	}

	/**
	 * Type string of the literal value. E.g. "BOOLEAN".
	 *
	 * @param typeString type string describing the value
	 */
	public LiteralValue of(String typeString) {
		this.typeInfo = typeString;
		return this;
	}

	/**
	 * Literal BOOLEAN value.
	 *
	 * @param value literal BOOLEAN value
	 */
	public LiteralValue value(boolean value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal INT value.
	 *
	 * @param value literal INT value
	 */
	public LiteralValue value(int value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal DOUBLE value.
	 *
	 * @param value literal DOUBLE value
	 */
	public LiteralValue value(double value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal FLOAT value.
	 *
	 * @param value literal FLOAT value
	 */
	public LiteralValue value(float value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal value either for an explicit VARCHAR type or automatically derived type.
	 *
	 * <p>If no type is set, the type is automatically derived from the value. Currently,
	 * this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR.
	 *
	 * @param value literal value
	 */
	public LiteralValue value(String value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal BIGINT value.
	 *
	 * @param value literal BIGINT value
	 */
	public LiteralValue value(long value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal TINYINT value.
	 *
	 * @param value literal TINYINT value
	 */
	public LiteralValue value(byte value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal SMALLINT value.
	 *
	 * @param value literal SMALLINT value
	 */
	public LiteralValue value(short value) {
		this.value = value;
		return this;
	}

	/**
	 * Literal DECIMAL value.
	 *
	 * @param value literal DECIMAL value
	 */
	public LiteralValue value(BigDecimal value) {
		this.value = value;
		return this;
	}

	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		addPropertiesWithPrefix(HierarchyDescriptorValidator.EMPTY_PREFIX, properties);
		return properties.asMap();
	}

	@Override
	public void addPropertiesWithPrefix(String keyPrefix, DescriptorProperties properties) {
		if (typeInfo != null) {
			properties.putString(keyPrefix + "type", typeInfo);
			if (value != null) {
				properties.putString(keyPrefix + "value", String.valueOf(value));
			}
		} else {
			// do not allow values in top-level
			if (keyPrefix.equals(HierarchyDescriptorValidator.EMPTY_PREFIX)) {
				throw new ValidationException(
						"Literal values with implicit type must not exist in the top level of a hierarchy.");
			}
			if (value != null) {
				properties.putString(keyPrefix.substring(0, keyPrefix.length() - 1), String.valueOf(value));
			}
		}
	}
}
