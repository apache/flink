package eu.stratosphere.pact.example.relational.contracts.tpch9;


import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.types.tpch9.*;
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
