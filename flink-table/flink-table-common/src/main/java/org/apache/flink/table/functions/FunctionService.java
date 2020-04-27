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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ClassInstanceValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.descriptors.FunctionDescriptorValidator;
import org.apache.flink.table.descriptors.HierarchyDescriptorValidator;
import org.apache.flink.table.descriptors.LiteralValueValidator;
import org.apache.flink.table.descriptors.PythonFunctionValidator;
import org.apache.flink.table.functions.python.utils.PythonFunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service for creating configured instances of {@link UserDefinedFunction} using a
 * {@link FunctionDescriptor}.
 */
public class FunctionService {

	private static final Logger LOG = LoggerFactory.getLogger(FunctionService.class);

	/**
	 * Creates a user-defined function with the given properties and the current thread's
	 * context class loader.
	 *
	 * @param descriptor the descriptor that describes a function
	 * @return the generated user-defined function
	 */
	public static UserDefinedFunction createFunction(FunctionDescriptor descriptor) {
		return createFunction(descriptor, Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Creates a user-defined function with the given properties.
	 *
	 * @param descriptor the descriptor that describes a function
	 * @param classLoader the class loader to load the function and its parameter's classes
	 * @return the generated user-defined function
	 */
	public static UserDefinedFunction createFunction(
			FunctionDescriptor descriptor,
			ClassLoader classLoader) {
		return createFunction(descriptor, classLoader, true);
	}

	/**
	 * Creates a user-defined function with the given properties.
	 *
	 * @param descriptor the descriptor that describes a function
	 * @param classLoader the class loader to load the function and its parameter's classes
	 * @param performValidation whether or not the descriptor should be validated
	 * @return the generated user-defined function
	 */
	public static UserDefinedFunction createFunction(
			FunctionDescriptor descriptor,
			ClassLoader classLoader,
			boolean performValidation) {
		return createFunction(descriptor, classLoader, performValidation, new Configuration());
	}

	/**
	 * Creates a user-defined function with the given properties.
	 *
	 * @param descriptor the descriptor that describes a function
	 * @param classLoader the class loader to load the function and its parameter's classes
	 * @param performValidation whether or not the descriptor should be validated
	 * @param config the table configuration used to create the function
	 * @return the generated user-defined function
	 */
	public static UserDefinedFunction createFunction(
			FunctionDescriptor descriptor,
			ClassLoader classLoader,
			boolean performValidation,
			ReadableConfig config) {

		DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(descriptor.toProperties());

		// validate
		if (performValidation) {
			new FunctionDescriptorValidator().validate(properties);
		}

		Object instance;
		switch (properties.getString(FunctionDescriptorValidator.FROM)) {
			case FunctionDescriptorValidator.FROM_VALUE_CLASS:
				// instantiate
				instance = generateInstance(
					HierarchyDescriptorValidator.EMPTY_PREFIX,
					properties,
					classLoader);
				break;
			case FunctionDescriptorValidator.FROM_VALUE_PYTHON:
				String fullyQualifiedName = properties.getString(PythonFunctionValidator.FULLY_QUALIFIED_NAME);
				instance = PythonFunctionUtils.getPythonFunction(fullyQualifiedName, config);
				break;
			default:
				throw new ValidationException(String.format(
						"Unsupported function descriptor: %s",
						properties.getString(FunctionDescriptorValidator.FROM)));
		}

		if (!UserDefinedFunction.class.isAssignableFrom(instance.getClass())) {
			throw new ValidationException(String.format(
					"Instantiated class '%s' is not a user-defined function.",
					instance.getClass().getName()));
		}
		return (UserDefinedFunction) instance;
	}

	/**
	 * Recursively generate an instance of a class according the given properties.
	 *
	 * @param keyPrefix the prefix to fetch properties
	 * @param descriptorProperties the descriptor properties that contains the class type information
	 * @param classLoader the class loader to load the class
	 * @param <T> type fo the generated instance
	 * @return an instance of the class
	 */
	private static <T> T generateInstance(
			String keyPrefix,
			DescriptorProperties descriptorProperties,
			ClassLoader classLoader) {
		String instanceClassName = descriptorProperties.getString(
				keyPrefix + ClassInstanceValidator.CLASS);

		Class<T> instanceClass;
		try {
			//noinspection unchecked
			instanceClass = (Class<T>) Class.forName(instanceClassName, true, classLoader);
		} catch (Exception e) {
			// only log the cause to have clean error messages
			String msg = String.format(
					"Could not find class '%s' for creating an instance.", instanceClassName);
			LOG.error(msg, e);
			throw new ValidationException(msg);
		}

		String constructorPrefix = keyPrefix + ClassInstanceValidator.CONSTRUCTOR;

		List<Map<String, String>> constructorProps = descriptorProperties
				.getVariableIndexedProperties(constructorPrefix, new ArrayList<>());

		ArrayList<Object> parameterList = new ArrayList<>();
		for (int i = 0; i < constructorProps.size(); i++) {
			String constructorKey = constructorPrefix + "." + i + ".";
			// nested class instance
			if (constructorProps.get(i).containsKey(ClassInstanceValidator.CLASS)) {
				parameterList.add(generateInstance(
						constructorKey,
						descriptorProperties,
						classLoader));
			}
			// literal value
			else {
				Object literalValue = LiteralValueValidator.getValue(
						constructorKey, descriptorProperties);
				parameterList.add(literalValue);
			}
		}

		String parameterNames = parameterList.stream()
				.map(t -> t.getClass().getName())
				.reduce((s1, s2) -> s1 + ", " + s2)
				.orElse("");

		Constructor<T> constructor;
		try {
			constructor = instanceClass.getConstructor(
					parameterList.stream().map(Object::getClass).toArray(Class[]::new));
		} catch (Exception e) {
			// only log the cause to have clean error messages
			String msg = String.format(
					"Cannot find a public constructor with parameter types '%s' for '%s'.",
					parameterNames,
					instanceClassName);
			LOG.error(msg, e);
			throw new ValidationException(msg);
		}

		try {
			return constructor.newInstance(parameterList.toArray());
		} catch (Exception e) {
			// only log the cause to have clean error messages
			String msg = String.format(
					"Error while creating instance of class '%s' with parameter types '%s'.",
					instanceClassName,
					parameterNames);
			LOG.error(msg, e);
			throw new ValidationException(msg);
		}
	}
}
