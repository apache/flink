package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class OrderMap extends MapStub<PactInteger, Tuple, PactInteger, PactInteger> {
	
	private static Logger LOGGER = Logger.getLogger(OrderMap.class);
	
	/**
	 * Project "orders"
	 * 
	 * Output Schema:
	 *  Key: orderkey
	 *  Value: year (from date)
	 *
	 */
	@Override
	public void map(PactInteger partKey, Tuple inputTuple,
			Collector<PactInteger, PactInteger> output) {
		
		/* Extract the year from the date element of the order relation: */
		
		try {
			int year = Integer.parseInt(inputTuple.getStringValueAt(4).substring(0, 4));
			output.collect(partKey, new PactInteger(year));
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
