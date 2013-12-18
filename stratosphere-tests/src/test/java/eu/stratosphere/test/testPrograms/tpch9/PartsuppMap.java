/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.test.testPrograms.tpch9;

import eu.stratosphere.api.record.functions.MapFunction;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

public class PartsuppMap extends MapFunction {
	
	private Tuple inputTuple = new Tuple();
	
	/**
	 * Project "partsupp".
	 * 
	 * Output Schema:
	 *  Key: partkey
	 *  Value: (suppkey, supplycost)
	 *
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
		inputTuple = record.getField(1, inputTuple);
		inputTuple.project((0 << 0) | (1 << 1) | (0 << 2) | (1 << 3) | (0 << 4));
		record.setField(1, inputTuple);
		out.collect(record);
	}

}
