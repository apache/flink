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
package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.types.tpch9.IntPair;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class PartJoin extends MatchStub<PactInteger, PactNull, Tuple, IntPair, PactString> {

	private static Logger LOGGER = Logger.getLogger(PartJoin.class);
	
	/**
	 * Join "part" and "partsupp" by "partkey".
	 * 
	 * Output Schema:
	 *  Key: (partkey, suppkey)
	 *  Value: supplycost
	 *
	 */
	@Override
	public void match(PactInteger partKey, PactNull dummy, Tuple partSuppValue,
			Collector<IntPair, PactString> output) {
		
		try {
			IntPair newKey = new IntPair(partKey, new PactInteger(Integer.parseInt(partSuppValue.getStringValueAt(0))));
			String supplyCost = partSuppValue.getStringValueAt(1);
		
			output.collect(newKey, new PactString(supplyCost));
		} catch(Exception e) {
			LOGGER.error(e);
		}
	}

}
