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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.OutputFormatProvider;

import java.util.Optional;

/**
 * Parallelism provider for other connector providers. It allows to express a custom parallelism for
 * the connector runtime implementation. Otherwise the parallelism is determined by the planner.
 *
 * <p>Note: Currently, this interface can only work with {@code org.apache.flink.table.connector.sink.SinkFunctionProvider}
 * in {@code flink-table-api-java-bridge} module and {@link OutputFormatProvider}.
 */
@PublicEvolving
public interface ParallelismProvider {

	/**
	 * Returns the parallelism for this instance.
	 *
	 * <p>The parallelism denotes how many parallel instances of a source or sink will be spawned
	 * during the execution.
	 *
	 * @return empty if the connector does not provide a custom parallelism, then the planner will
	 *         decide the number of parallel instances by itself.
	 */
	default Optional<Integer> getParallelism() {
		return Optional.empty();
	}
}
