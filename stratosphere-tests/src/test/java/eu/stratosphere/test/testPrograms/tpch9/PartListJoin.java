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


import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class PartListJoin extends JoinFunction{

	private final StringIntPair amountYearPair = new StringIntPair();
	private final StringValue nationName = new StringValue();
	
	/**
	 * Join "filteredParts" and "suppliers" by "suppkey".
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	@Override
	public void match(Record value1, Record value2, Collector<Record> out) throws Exception
	{
		StringIntPair amountYearPair = value1.getField(1, this.amountYearPair);
		StringValue nationName = value2.getField(1, this.nationName);
		
		IntValue year = amountYearPair.getSecond();
		StringValue amount = amountYearPair.getFirst();
		StringIntPair key = new StringIntPair(nationName, year);
		value1.setField(0, key);
		value1.setField(1, amount);
		out.collect(value1);
	}

}
