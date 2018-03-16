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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * This interface marks operators as operators that execute user-defined functions (UDFs), such as
 * {@link org.apache.flink.api.common.functions.RichMapFunction}, {@link org.apache.flink.api.common.functions.RichReduceFunction},
 * or {@link org.apache.flink.api.common.functions.RichCoGroupFunction}.
 * The UDF operators stand in contrast to operators that execute built-in operations, like aggregations.
 */
@Public
public interface UdfOperator<O extends UdfOperator<O>> {

	// --------------------------------------------------------------------------------------------
	// Accessors
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the configuration parameters that will be passed to the UDF's open method
	 * {@link org.apache.flink.api.common.functions.AbstractRichFunction#open(Configuration)}.
	 * The configuration is set via the {@link #withParameters(Configuration)}
	 * method.
	 *
	 * @return The configuration parameters for the UDF.
	 */
	Configuration getParameters();

	/**
	 * Gets the broadcast sets (name and data set) that have been added to context of the UDF.
	 * Broadcast sets are added to a UDF via the method {@link #withBroadcastSet(DataSet, String)}.
	 *
	 * @return The broadcast data sets that have been added to this UDF.
	 */
	@Internal
	Map<String, DataSet<?>> getBroadcastSets();

	/**
	 * Gets the semantic properties that have been set for the user-defined functions (UDF).
	 *
	 * @return The semantic properties of the UDF.
	 */
	@Internal
	SemanticProperties getSemanticProperties();

	// --------------------------------------------------------------------------------------------
	// Fluent API methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the configuration parameters for the UDF. These are optional parameters that are passed
	 * to the UDF in the {@link org.apache.flink.api.common.functions.AbstractRichFunction#open(Configuration)} method.
	 *
	 * @param parameters The configuration parameters for the UDF.
	 * @return The operator itself, to allow chaining function calls.
	 */
	O withParameters(Configuration parameters);

	/**
	 * Adds a certain data set as a broadcast set to this operator. Broadcasted data sets are available at all
	 * parallel instances of this operator. A broadcast data set is registered under a certain name, and can be
	 * retrieved under that name from the operators runtime context via
	 * {@link org.apache.flink.api.common.functions.RuntimeContext#getBroadcastVariable(String)}.
	 *
	 * <p>The runtime context itself is available in all UDFs via
	 * {@link org.apache.flink.api.common.functions.AbstractRichFunction#getRuntimeContext()}.
	 *
	 * @param data The data set to be broadcast.
	 * @param name The name under which the broadcast data set retrieved.
	 * @return The operator itself, to allow chaining function calls.
	 */
	O withBroadcastSet(DataSet<?> data, String name);
}
