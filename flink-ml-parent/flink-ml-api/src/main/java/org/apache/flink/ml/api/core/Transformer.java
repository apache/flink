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

package org.apache.flink.ml.api.core;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * A transformer is a {@link PipelineStage} that transforms an input {@link Table} to a result
 * {@link Table}.
 *
 * @param <T> The class type of the Transformer implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
@PublicEvolving
public interface Transformer<T extends Transformer<T>> extends PipelineStage<T> {
	/**
	 * Applies the transformer on the input table, and returns the result table.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table to be transformed
	 * @return the transformed table
	 */
	Table transform(TableEnvironment tEnv, Table input);
}
