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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Either;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.types.Either.Left;
import static org.apache.flink.types.Either.Right;

/**
 * Descriptor for a class instance. A class instance is a Java/Scala object created from a class
 * with a public constructor (with or without parameters).
 */
@PublicEvolving
public class ClassInstance extends HierarchyDescriptor {

	private String className;
	// the parameter is either a literal value or the instance of a class
	private List<Either<LiteralValue, ClassInstance>> constructor = new ArrayList<>();

	/**
	 * Sets the fully qualified class name for creating an instance.
	 *
	 * <p>E.g. org.example.MyClass or org.example.MyClass$StaticInnerClass
	 *
	 * @param className fully qualified class name
	 */
	public ClassInstance of(String className) {
		this.className = className;
		return this;
	}

	/**
	 * Adds a constructor parameter value of literal type. The type is automatically derived from
	 * the value. Currently, this is supported for: BOOLEAN, INT, DOUBLE, and VARCHAR. Expression
	 * values are not allowed.
	 *
	 * <p>Examples:
	 * - "true", "false" -> BOOLEAN
	 * - "42", "-5" -> INT
	 * - "2.0", "1234.222" -> DOUBLE
	 * - VARCHAR otherwise
	 *
	 * <p>For other types and explicit type declaration use {@link #parameter(String, String)} or
	 * {@link #parameter(TypeInformation, String)}.
	 */
	public ClassInstance parameterString(String valueString) {
		constructor.add(Left(new LiteralValue().value(valueString)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of literal type. The type is explicitly defined using a
	 * type string such as VARCHAR, FLOAT, BOOLEAN, INT, BIGINT, etc. The value is parsed
	 * accordingly. Expression values are not allowed.
	 *
	 * @param typeString the type string that define how to parse the given value string
	 * @param valueString the literal value to be parsed
	 */
	public ClassInstance parameter(String typeString, String valueString) {
		constructor.add(Left(new LiteralValue().of(typeString).value(valueString)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of literal type. The type is explicitly defined using
	 * type information. The value is parsed accordingly. Expression values are not allowed.
	 *
	 * @param typeInfo the type that define how to parse the given value string
	 * @param valueString the literal value to be parsed
	 */
	public ClassInstance parameter(TypeInformation<?> typeInfo, String valueString) {
		constructor.add(Left(new LiteralValue().of(typeInfo).value(valueString)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of BOOLEAN type.
	 *
	 * @param value BOOLEAN value
	 */
	public ClassInstance parameter(boolean value) {
		constructor.add(Left(new LiteralValue().of(Types.BOOLEAN).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of DOUBLE type.
	 *
	 * @param value DOUBLE value
	 */
	public ClassInstance parameter(double value) {
		constructor.add(Left(new LiteralValue().of(Types.DOUBLE).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of FLOAT type.
	 *
	 * @param value FLOAT value
	 */
	public ClassInstance parameter(float value) {
		constructor.add(Left(new LiteralValue().of(Types.FLOAT).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of INT type.
	 *
	 * @param value INT value
	 */
	public ClassInstance parameter(int value) {
		constructor.add(Left(new LiteralValue().of(Types.INT).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of VARCHAR type.
	 *
	 * @param value VARCHAR value
	 */
	public ClassInstance parameter(String value) {
		constructor.add(Left(new LiteralValue().of(Types.STRING).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of BIGINT type.
	 *
	 * @param value BIGINT value
	 */
	public ClassInstance parameter(long value) {
		constructor.add(Left(new LiteralValue().of(Types.LONG).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of TINYINT type.
	 *
	 * @param value TINYINT value
	 */
	public ClassInstance parameter(byte value) {
		constructor.add(Left(new LiteralValue().of(Types.BYTE).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of SMALLINT type.
	 *
	 * @param value SMALLINT value
	 */
	public ClassInstance parameter(short value) {
		constructor.add(Left(new LiteralValue().of(Types.SHORT).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of DECIMAL type.
	 *
	 * @param value DECIMAL value
	 */
	public ClassInstance parameter(BigDecimal value) {
		constructor.add(Left(new LiteralValue().of(Types.BIG_DEC).value(value)));
		return this;
	}

	/**
	 * Adds a constructor parameter value of a class instance (i.e. a Java object with a public
	 * constructor).
	 *
	 * @param classInstance description of a class instance (i.e. a Java object with a public
	 * constructor).
	 */
	public ClassInstance parameter(ClassInstance classInstance) {
		constructor.add(Right(classInstance));
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		addPropertiesWithPrefix(HierarchyDescriptorValidator.EMPTY_PREFIX, properties);
		return properties.asMap();
	}

	/**
	 * Internal method for properties conversion.
	 */
	@Override
	public void addPropertiesWithPrefix(String keyPrefix, DescriptorProperties properties) {
		if (className != null) {
			properties.putString(keyPrefix + ClassInstanceValidator.CLASS, className);
		}
		for (int i = 0; i < constructor.size(); ++i) {
			Either<LiteralValue, ClassInstance> either = constructor.get(i);
			String keyPrefixWithIdx = keyPrefix + ClassInstanceValidator.CONSTRUCTOR + "." + i + ".";
			if (either.isLeft()) {
				either.left().addPropertiesWithPrefix(keyPrefixWithIdx, properties);
			} else {
				either.right().addPropertiesWithPrefix(keyPrefixWithIdx, properties);
			}
		}
	}

}
