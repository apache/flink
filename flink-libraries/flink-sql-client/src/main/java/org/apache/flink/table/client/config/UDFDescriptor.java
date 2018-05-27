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

package org.apache.flink.table.client.config;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.descriptors.ClassTypeDescriptor;
import org.apache.flink.table.descriptors.ClassTypeValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.descriptors.FunctionValidator;
import org.apache.flink.table.descriptors.PrimitiveTypeDescriptor;
import org.apache.flink.table.descriptors.PrimitiveTypeValidator;
import org.apache.flink.table.typeutils.TypeStringUtils;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.UDFDescriptor.From.CLASS;

/**
 * Descriptor for user-defined functions.
 */
public class UDFDescriptor extends FunctionDescriptor {

	private static final String FROM = "from";

	private From from;

	private UDFDescriptor(String name, From from) {
		super(name);
		this.from = from;
	}

	public From getFrom() {
		return from;
	}

	/**
	 * Create a UDF descriptor with the given config.
	 */
	public static UDFDescriptor create(Map<String, Object> config) {
		if (!config.containsKey(FunctionValidator.FUNCTION_NAME())) {
			throw new SqlClientException("The 'name' attribute of a function is missing.");
		}

		final Object name = config.get(FunctionValidator.FUNCTION_NAME());
		if (!(name instanceof String) || ((String) name).length() <= 0) {
			throw new SqlClientException("Invalid function name '" + name + "'.");
		}

		From fromValue = CLASS;

		if (config.containsKey(FROM)) {
			final Object from = config.get(FROM);

			if (!(from instanceof String)) {
				throw new SqlClientException("Invalid 'from' value '" + from.toString() + "'.");
			}

			try {
				fromValue = From.valueOf(((String) from).toUpperCase());
			} catch (IllegalArgumentException ex) {
				throw new SqlClientException("Unknown 'from' value '" + from.toString() + "'.");
			}
		}

		switch (fromValue) {
			case CLASS:
				ClassTypeDescriptor descriptor = generateFunction(
						(String) config.get("class"), (List<?>) config.get("constructor"));
				UDFDescriptor udf = new UDFDescriptor((String) name, CLASS);
				udf.setClassDescriptor(descriptor);
				return udf;
			default:
				throw new SqlClientException("The from attribute can only be \"class\" now.");
		}

	}

	@SuppressWarnings("unchecked")
	private static ClassTypeDescriptor generateFunction(String className, List<?> constructorList) {
		ClassTypeDescriptor classTypeDescriptor = new ClassTypeDescriptor();
		classTypeDescriptor.setClassName(className);
		Map map;
		for (Object field : constructorList) {
			if (field instanceof Map) {
				map = (Map) field;
				if (map.containsKey(PrimitiveTypeValidator.PRIMITIVE_TYPE())) {
					String typeStr = (String) map.get(PrimitiveTypeValidator.PRIMITIVE_TYPE());
					Object value = map.get(PrimitiveTypeValidator.PRIMITIVE_VALUE());
					TypeInformation typeInfo = TypeStringUtils.readTypeInfo(typeStr);
					if (!(typeInfo instanceof BasicTypeInfo)) {
						throw new SqlClientException("Only basic types are supported.");
					}
					PrimitiveTypeDescriptor basicTypeField = new PrimitiveTypeDescriptor();
					basicTypeField.setType((BasicTypeInfo) typeInfo).setValue(value);
					classTypeDescriptor.addConstructorField(basicTypeField);
				} else if (map.containsKey(ClassTypeValidator.CLASS())) {
					String clazz = (String) map.get(ClassTypeValidator.CLASS());
					List<?> constructor = (List<?>) map.get(ClassTypeValidator.CLASS_CONSTRUCTOR());
					classTypeDescriptor.addConstructorField(generateFunction(clazz, constructor));
				}
			} else if (field instanceof List) {
				throw new SqlClientException("Unsupported list field " + field);
			} else {
				PrimitiveTypeDescriptor basicTypeField = new PrimitiveTypeDescriptor();
				if (field instanceof Integer) {
					basicTypeField.setType(BasicTypeInfo.INT_TYPE_INFO);
				} if (field instanceof Double) {
					basicTypeField.setType(BasicTypeInfo.DOUBLE_TYPE_INFO);
				} if (field instanceof String) {
					basicTypeField.setType(BasicTypeInfo.STRING_TYPE_INFO);
				} else if (field instanceof Boolean) {
					basicTypeField.setType(BasicTypeInfo.BOOLEAN_TYPE_INFO);
				}
				// TODO more types should be supported?
				basicTypeField.setValue(field);
				classTypeDescriptor.addConstructorField(basicTypeField);
			}
		}
		return classTypeDescriptor;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void addProperties(DescriptorProperties properties) {
		properties.putString(FROM, from.toString().toLowerCase());
		super.addProperties(properties);
	}

	enum From {
		CLASS
	}
}
