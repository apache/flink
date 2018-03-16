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

package org.apache.flink.graph.drivers.transform;

import org.apache.flink.graph.drivers.parameter.Parameterized;

/**
 * A transform is executed both before and after algorithm execution. Multiple
 * transforms may be applied and the order of execution is as listed by the
 * input then algorithm components. The input is transformed in given order and
 * the algorithm result is transformed in reverse order.
 *
 * @param <II> input transformation input type
 * @param <IO> input transformation output type
 * @param <RI> result transformation input type
 * @param <RO> input transformation output type
 */
public interface Transform<II, IO, RI, RO>
extends Parameterized {

	/**
	 * A human-readable identifier summarizing the transform and configuration.
	 *
	 * @return the transform identifier
	 */
	String getIdentity();

	/**
	 * Apply the forward transformation to the input graph.
	 *
	 * @param input transformation input
	 * @return transformation output
	 * @throws Exception
	 */
	IO transformInput(II input) throws Exception;

	/**
	 * Apply the reverse transformation to the algorithm result.
	 *
	 * @param result transformation input
	 * @return transformation output
	 * @throws Exception
	 */
	RO transformResult(RI result) throws Exception;
}
