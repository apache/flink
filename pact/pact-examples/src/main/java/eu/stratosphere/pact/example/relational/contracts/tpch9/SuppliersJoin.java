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
package eu.stratosphere.pact.example.relational.contracts.tpch9;


import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;


public class SuppliersJoin extends MatchStub<PactInteger, PactInteger, Tuple, PactInteger, PactString> {
	
	private static Logger LOGGER = Logger.getLogger(SuppliersJoin.class);
	
	/**
	 * Join "nation" and "supplier" by "nationkey".
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: "nation" (name of the nation)
	 *
	 */
	@Override
	public void match(PactInteger partKey, PactInteger suppKey, Tuple nationVal,
			Collector<PactInteger, PactString> output) {
		
		try {
			PactString nationName = new PactString(nationVal.getStringValueAt(1));
			output.collect(suppKey, nationName);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}

	}

}
