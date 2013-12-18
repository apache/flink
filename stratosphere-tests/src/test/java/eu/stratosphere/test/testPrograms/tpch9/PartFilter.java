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
import eu.stratosphere.types.PactNull;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

public class PartFilter extends MapFunction {

	private final Tuple inputTuple = new Tuple();
	
	private static String COLOR = "green";
	
	/**
	 * Filter and project "part".
	 * The parts are filtered by "name LIKE %green%".
	 * 
	 * Output Schema:
	 *  Key: partkey
	 *  Value: (empty)
	 *
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> out) throws Exception
	{
		Tuple inputTuple = record.getField(1, this.inputTuple);
		if (inputTuple.getStringValueAt(1).indexOf(COLOR) != -1) {
			record.setField(1, PactNull.getInstance());
			out.collect(record);
		}
	}
}
