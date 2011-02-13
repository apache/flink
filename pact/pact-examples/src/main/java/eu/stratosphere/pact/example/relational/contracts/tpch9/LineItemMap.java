package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.contracts.tpch4.Join;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class LineItemMap extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {
	
	private static Logger LOGGER = Logger.getLogger(Join.class);
	
	/**
	 * Filter "lineitem".
	 * 
	 * Output Schema:
	 *  Key: orderkey
	 *  Value: (partkey, suppkey, quantity, price)
	 *
	 */
	@Override
	public void map(PactInteger partKey, Tuple inputTuple,
			Collector<PactInteger, Tuple> output) {
		
		/* Extract the year from the date element of the order relation: */
		
		try {
			/* pice = extendedprice * (1 - discount): */
			float price = Float.parseFloat(inputTuple.getStringValueAt(5)) * (1 - Float.parseFloat(inputTuple.getStringValueAt(6)));
			/* Project (orderkey | partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, ...) to (partkey, suppkey, quantity): */
			inputTuple.project((0 << 0) | (1 << 1) | (1 << 2) | (0 << 3) | (1 << 4));
			inputTuple.addAttribute("" + price);
			output.collect(partKey, inputTuple);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
