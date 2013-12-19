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

package eu.stratosphere.test.testPrograms.tpch9;

import eu.stratosphere.api.record.functions.MapFunction;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class SupplierMap extends MapFunction {
	
	private IntValue suppKey = new IntValue();
	private Tuple inputTuple = new Tuple();
	
	/**
	 * Project "supplier".
	 * 
	 * Output Schema:
	 *  Key: nationkey
	 *  Value: suppkey
	 *
	 */
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		suppKey = record.getField(0, suppKey);
		inputTuple = record.getField(1, inputTuple);
		
		/* Project (suppkey | name, address, nationkey, phone, acctbal, comment): */
		IntValue nationKey = new IntValue(Integer.parseInt(inputTuple.getStringValueAt(3)));
		
		record.setField(0, nationKey);
		record.setField(1, suppKey);
		
		out.collect(record);
	}

}
