package eu.stratosphere.pact.example.relational.contracts.tpch9;

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
