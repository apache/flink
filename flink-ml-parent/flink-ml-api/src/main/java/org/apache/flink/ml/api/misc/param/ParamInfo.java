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

package org.apache.flink.ml.api.misc.param;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * Definition of a parameter, including name, type, default value, validator and so on.
 *
 * <p>A parameter can either be optional or non-optional.
 * <ul>
 *     <li>
 *         A non-optional parameter should not have a default value. Instead, its value must be provided by the users.
 *     </li>
 *     <li>
 *         An optional parameter may or may not have a default value.
 *     </li>
 * </ul>
 *
 * <p>Please see {@link Params#get(ParamInfo)} and {@link Params#contains(ParamInfo)} for more details about the behavior.
 *
 * <p>A parameter may have aliases in addition to the parameter name for convenience and compatibility purposes. One
 * should not set values for both parameter name and an alias. One and only one value should be set either under
 * the parameter name or one of the alias.
 *
 * @param <V> the type of the param value
 */
@PublicEvolving
public class ParamInfo<V> {
	private final String name;
	private final String[] alias;
	private final String description;
	private final boolean isOptional;
	private final boolean hasDefaultValue;
	private final V defaultValue;
	private final ParamValidator<V> validator;
	private final Class<V> valueClass;

	ParamInfo(String name, String[] alias, String description, boolean isOptional,
			boolean hasDefaultValue, V defaultValue,
			ParamValidator<V> validator, Class<V> valueClass) {
		this.name = name;
		this.alias = alias;
		this.description = description;
		this.isOptional = isOptional;
		this.hasDefaultValue = hasDefaultValue;
		this.defaultValue = defaultValue;
		this.validator = validator;
		this.valueClass = valueClass;
	}

	/**
	 * Returns the name of the parameter. The name must be unique in the stage the ParamInfo
	 * belongs to.
	 *
	 * @return the name of the parameter
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the aliases of the parameter. The alias will be an empty string array by default.
	 *
	 * @return the aliases of the parameter
	 */
	public String[] getAlias() {
		Preconditions.checkNotNull(alias);
		return alias;
	}

	/**
	 * Returns the description of the parameter.
	 *
	 * @return the description of the parameter
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Returns whether the parameter is optional.
	 *
	 * @return {@code true} if the param is optional, {@code false} otherwise
	 */
	public boolean isOptional() {
		return isOptional;
	}

	/**
	 * Returns whether the parameter has a default value. Since {@code null} may also be a valid
	 * default value of a parameter, the return of getDefaultValue may be {@code null} even when
	 * this method returns true.
	 *
	 * @return {@code true} if the param is has a default value(even if it's a {@code null}), {@code
	 * false} otherwise
	 */
	public boolean hasDefaultValue() {
		return hasDefaultValue;
	}

	/**
	 * Returns the default value of the parameter. The default value should be defined whenever
	 * possible. The default value can be a {@code null} even if hasDefaultValue returns true.
	 *
	 * @return the default value of the param, {@code null} if not defined
	 */
	public V getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Returns the validator to validate the value of the parameter.
	 *
	 * @return the validator to validate the value of the parameter.
	 */
	public ParamValidator<V> getValidator() {
		return validator;
	}

	/**
	 * Returns the class of the param value. It's usually needed in serialization.
	 *
	 * @return the class of the param value
	 */
	public Class<V> getValueClass() {
		return valueClass;
	}
}
