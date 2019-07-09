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

package org.apache.flink.ml.api.misc.factory;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.Map;

/**
 * Base interface of a factory to load with {@link BaseFactoryLoader}.
 */
@PublicEvolving
public interface BaseFactory {

	/**
	 * Specifies the context that this factory is implemented for. BaseFactoryLoader matches this
	 * factory only if all properties defined here are all provided with the required values.
	 */
	Map<String, String> requiredContext();

	/**
	 * Specifies the properties that this factory requires. BaseFactoryLoader matches this factory
	 * only if all properties defined here are all provided. This may contain those defined in
	 * requiredContext or not, results will be the same.
	 */
	List<String> requiredProperties();

	/**
	 * Specifies the properties that this factory supports. BaseFactoryLoader matches this factory
	 * only if no properties outside the specified list are provided. A property defined in
	 * requiredContext and requiredProperties will automatically added if it's not in this list.
	 */
	List<String> supportedProperties();
}
