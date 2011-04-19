/***********************************************************************************************************************
 *
 * Copyright (C) 2011 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.test.testPrograms.tpch9;

import org.apache.log4j.Logger;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class PartFilter extends MapStub<PactInteger, Tuple, PactInteger, PactNull> {

	private static String COLOR = "green";
	
	private static Logger LOGGER = Logger.getLogger(PartFilter.class);
	
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
	public void map(PactInteger partKey, Tuple inputTuple,
			Collector<PactInteger, PactNull> output) {
		
		try {
			if(inputTuple.getStringValueAt(1).indexOf(COLOR) != -1)
				output.collect(partKey, new PactNull());
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
