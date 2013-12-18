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


import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;
import eu.stratosphere.util.Collector;


public class SuppliersJoin extends JoinFunction {
	
	private PactInteger suppKey = new PactInteger();
	private Tuple nationVal = new Tuple();
	
	/**
	 * Join "nation" and "supplier" by "nationkey".
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: "nation" (name of the nation)
	 *
	 */
	@Override
	public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out)
			throws Exception {
		suppKey = value1.getField(1, suppKey);
		nationVal = value2.getField(1, nationVal);
		
		PactString nationName = new PactString(nationVal.getStringValueAt(1));
		
		value1.setField(0, suppKey);
		value1.setField(1, nationName);
		
		out.collect(value1);
		
	}

}
