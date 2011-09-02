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

package eu.stratosphere.pact.test.testPrograms.tpch9;


import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;

public class PartListJoin extends MatchStub<PactInteger, StringIntPair, PactString, StringIntPair, PactString> {
	
	private static Logger LOGGER = Logger.getLogger(PartListJoin.class);
	
	/**
	 * Join "filteredParts" and "suppliers" by "suppkey".
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	@Override
	public void match(PactInteger suppKey, StringIntPair amountYearPair, PactString nationName,
			Collector<StringIntPair, PactString> output) {
		
		try {
			PactInteger year = amountYearPair.getSecond();
			PactString amount = amountYearPair.getFirst();
			StringIntPair key = new StringIntPair(nationName, year);
			output.collect(key, amount);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}

	}

}
