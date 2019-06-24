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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.DataSet;

/**
 * An OperatorTransformation represents a single operator within a {@link Savepoint}.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public abstract class OperatorTransformation {

	/**
	 * Create a new {@link OperatorTransformation} from a {@link DataSet}.
	 *
	 * @param dataSet A dataset of elements.
	 * @param <T> The type of the input.
	 * @return A {@link OneInputOperatorTransformation}.
	 */
	public static <T> OneInputOperatorTransformation<T> bootstrapWith(DataSet<T> dataSet) {
		return new OneInputOperatorTransformation<>(dataSet);
	}
}

