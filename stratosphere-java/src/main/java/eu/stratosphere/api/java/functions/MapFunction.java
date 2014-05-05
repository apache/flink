/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMap;

/**
 * This abstract class is the base for all user-defined "tuple-at-a-time"
 * operations.
 * The user has to implement the map()-method with custom code.
 * 
 * The {@link AbstractFunction#open(eu.stratosphere.configuration.Configuration)} and
 * {@link AbstractFunction#close()} methods can be used for setup tasks (such as creating {@link Accumulator}s)
 *
 * @param <IN> Type of incoming objects
 * @param <OUT> Type of outgoing objects
 */
public abstract class MapFunction<IN, OUT> extends AbstractFunction implements GenericMap<IN, OUT> {

	private static final long serialVersionUID = 1L;

	public abstract OUT map(IN value) throws Exception;
}
