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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Optional;

/**
 * Provides details about a function call during {@link TypeInference} and allows to mutate argument
 * data types for {@link InputTypeStrategy}.
 *
 * <p>Note: In particular, this method allows to enrich {@link DataTypes#NULL()} to a meaningful data
 * type or modify a {@link DataType}'s conversion class.
 */
@PublicEvolving
public interface MutableCallContext extends CallContext {

	/**
	 * Mutates the data type of an argument at the given position.
	 */
	void mutateArgumentDataType(int pos, DataType newDataType);

	/**
	 * Returns the inferred output data type of the function call.
	 *
	 * <p>It does this by inferring the input argument data type of a wrapping call (if available)
	 * where this function call is an argument. For example, {@code takes_string(this_function(NULL))}
	 * would lead to a {@link DataTypes#STRING()} because the wrapping call expects a string argument.
	 */
	Optional<DataType> getOutputDataType();
}
