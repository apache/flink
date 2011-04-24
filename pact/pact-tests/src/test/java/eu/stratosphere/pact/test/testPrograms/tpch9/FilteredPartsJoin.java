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
import eu.stratosphere.pact.example.relational.util.Tuple;


public class FilteredPartsJoin extends MatchStub<IntPair, PactString, Tuple, PactInteger, StringIntPair> {
	
	private static Logger LOGGER = Logger.getLogger(FilteredPartsJoin.class);
	
	/**
	 * Join together parts and orderedParts by matching partkey and suppkey.
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: (amount, year)
	 *
	 */
	@Override
	public void match(IntPair partAndSupplierKey, PactString supplyCostStr, Tuple ordersValue,
			Collector<PactInteger, StringIntPair> output) {
		
		try {
			PactInteger year = new PactInteger(Integer.parseInt(ordersValue.getStringValueAt(0)));
			float quantity = Float.parseFloat(ordersValue.getStringValueAt(1));
			float price = Float.parseFloat(ordersValue.getStringValueAt(2));
			float supplyCost = Float.parseFloat(supplyCostStr.toString());
			float amount = price - supplyCost * quantity;
			
			/* Push (supplierKey, (amount, year)): */
			output.collect(partAndSupplierKey.getSecond(), new StringIntPair(new PactString("" + amount), year));
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}

	}

}
