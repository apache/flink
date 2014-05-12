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

package eu.stratosphere.test.recordJobs.relational.query9Util;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.test.recordJobs.util.Tuple;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class PartJoin extends JoinFunction {
	
	private final Tuple partSuppValue = new Tuple();
	private final IntValue partKey = new IntValue();
	
	/**
	 * Join "part" and "partsupp" by "partkey".
	 * 
	 * Output Schema:
	 *  Key: (partkey, suppkey)
	 *  Value: supplycost
	 *
	 */
	@Override
	public void join(Record value1, Record value2, Collector<Record> out)
			throws Exception {

		IntValue partKey = value1.getField(0, this.partKey);
		Tuple partSuppValue = value2.getField(1, this.partSuppValue);
		
		IntPair newKey = new IntPair(partKey, new IntValue(Integer.parseInt(partSuppValue.getStringValueAt(0))));
		String supplyCost = partSuppValue.getStringValueAt(1);
		
		value1.setField(0, newKey);
		value1.setField(1, new StringValue(supplyCost));
		out.collect(value1);
		
	}

}
