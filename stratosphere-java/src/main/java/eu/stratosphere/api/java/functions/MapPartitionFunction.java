/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.java.functions;


import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.GenericMapPartition;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

public abstract class MapPartitionFunction<IN, OUT> extends AbstractFunction implements GenericMapPartition<IN, OUT> {

	private static final long serialVersionUID = 1L;
	/**
	 *
	 * @param records All records for the mapper
	 * @param out The collector to hand results to.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract void mapPartition(Iterator<IN> records, Collector<OUT> out) throws Exception;
}
