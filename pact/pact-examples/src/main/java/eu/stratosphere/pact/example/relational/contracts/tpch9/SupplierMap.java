package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class SupplierMap extends MapStub<PactInteger, Tuple, PactInteger, PactInteger> {
	
	private static Logger LOGGER = Logger.getLogger(SupplierMap.class);
	
	/**
	 * Project "supplier".
	 * 
	 * Output Schema:
	 *  Key: nationkey
	 *  Value: suppkey
	 *
	 */
	@Override
	public void map(PactInteger suppKey, Tuple inputTuple,
			Collector<PactInteger, PactInteger> output) {
		
		try {
			/* Project (suppkey | name, address, nationkey, phone, acctbal, comment): */
			PactInteger nationKey = new PactInteger(Integer.parseInt(inputTuple.getStringValueAt(3)));
			output.collect(nationKey, suppKey);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
