package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.types.tpch9.IntPair;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class OrderedPartsJoin extends MatchStub<PactInteger, PactInteger, Tuple, IntPair, Tuple> {

	private static Logger LOGGER = Logger.getLogger(OrderedPartsJoin.class);
	
	/**
	 * Join "orders" and "lineitem" by "orderkey".
	 * 
	 * Output Schema:
	 *  Key: (partkey, suppkey)
	 *  Value: (year, quantity, price)
	 *
	 */
	@Override
	public void match(PactInteger orderKey, PactInteger year, Tuple lineItem,
			Collector<IntPair, Tuple> output) {
		
		try {
			/* (partkey, suppkey) from lineItem: */
			IntPair newKey = new IntPair(new PactInteger(Integer.parseInt(lineItem.getStringValueAt(0))), new PactInteger(Integer.parseInt(lineItem.getStringValueAt(1))));
			Tuple newValue = new Tuple();
			newValue.addAttribute(year.toString()); // year
			newValue.addAttribute(lineItem.getStringValueAt(2)); // quantity
			newValue.addAttribute(lineItem.getStringValueAt(3)); // price		
			output.collect(newKey, newValue);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
