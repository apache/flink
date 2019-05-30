/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params;

import org.apache.flink.ml.params.validators.Validator;

import java.io.Serializable;

/**
 * Definition of a parameter, including name, type, description, isOptional and so on.
 *
 * @param <V> the type of the parameter value
 */
public class ParamInfo<V> implements Serializable {
	/**
	 * the name of this parameter.
	 */
	final String name;

	/**
	 * the alias for name.
	 */
	final String[] alias;

	/**
	 * the help document of this parameter.
	 */
	final String description;

	/**
	 * whether this parameter is optional or required.
	 * true: optional
	 * false: required
	 */
	final Boolean optional;

	/**
	 * has default value or not.
	 */
	final Boolean hasDefaultValue;
	/**
	 * the default value of this parameter.
	 */
	final V defaultValue;

	/**
	 * the type class of this parameter.
	 */
	final Class <V> valueClass;

	/**
	 * the validator of this parameter.
	 */
	final Validator <V> validator;

	public ParamInfo(String name, String description, Boolean optional, Class <V> valueClass) {
		this.name = name;
		this.alias = null;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = false;
		this.defaultValue = null;
		this.valueClass = valueClass;
		this.validator = null;
	}

	public ParamInfo(String name, String[] alias, String description, Boolean optional, Class <V> valueClass) {
		this.name = name;
		this.alias = alias;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = false;
		this.defaultValue = null;
		this.valueClass = valueClass;
		this.validator = null;
	}

	public ParamInfo(String name, String description, Boolean optional, Class <V> valueClass, Validator <V>
		validator) {
		this.name = name;
		this.alias = null;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = false;
		this.defaultValue = null;
		this.valueClass = valueClass;
		this.validator = validator;
	}

	public ParamInfo(
		String name, String[] alias, String description, Boolean optional, Class <V> valueClass,
		Validator <V> validator) {
		this.name = name;
		this.alias = alias;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = false;
		this.defaultValue = null;
		this.valueClass = valueClass;
		this.validator = validator;
	}

	public ParamInfo(String name, String description, Boolean optional, V defaultValue, Class <V> valueClass) {
		this.name = name;
		this.alias = null;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = true;
		this.defaultValue = defaultValue;
		this.valueClass = valueClass;
		this.validator = null;
	}

	public ParamInfo(
		String name, String[] alias, String description, Boolean optional, V defaultValue, Class <V> valueClass) {
		this.name = name;
		this.alias = alias;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = true;
		this.defaultValue = defaultValue;
		this.valueClass = valueClass;
		this.validator = null;
	}

	public ParamInfo(
		String name, String description, Boolean optional, V defaultValue, Class <V> valueClass,
		Validator <V> validator) {
		this.name = name;
		this.alias = null;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = true;
		this.defaultValue = defaultValue;
		this.valueClass = valueClass;
		this.validator = validator;
	}

	public ParamInfo(
		String name, String[] alias, String description, Boolean optional, V defaultValue, Class <V> valueClass,
		Validator <V> validator) {
		this.name = name;
		this.alias = alias;
		this.description = description;
		this.optional = optional;
		this.hasDefaultValue = true;
		this.defaultValue = defaultValue;
		this.valueClass = valueClass;
		this.validator = validator;
	}

	public String getName() {
		return name;
	}

	public String[] getAlias() {
		return alias;
	}

	public String getDescription() {
		return description;
	}

	public Boolean hasDefaultValue() {
		return hasDefaultValue;
	}

	public V getDefaultValue() {
		return defaultValue;
	}

	public Validator <V> getValidator() {
		return validator;
	}

	public Class <V> getValueClass() {
		return valueClass;
	}

	public Boolean isOptional() {
		return optional;
	}

}
