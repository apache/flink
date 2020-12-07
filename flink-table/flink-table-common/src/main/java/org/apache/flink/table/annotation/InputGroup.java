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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

/**
 * A list of commonly used pre-defined groups of similar types for accepting more than just one data
 * type as an input argument in {@link DataTypeHint}s.
 *
 * <p>This list exposes a combination of {@link LogicalTypeFamily} and {@link InputTypeStrategies}
 * via annotations for convenient inline usage.
 */
@PublicEvolving
public enum InputGroup {

	/**
	 * Default if no group is specified.
	 */
	UNKNOWN,

	/**
	 * Enables input wildcards. Any data type can be passed. The behavior is equal to {@link InputTypeStrategies#ANY}.
	 *
	 * <p>Note: The class of the annotated element must be {@link Object} as this is the super class
	 * of all possibly passed data types.
	 */
	ANY
}
