package eu.stratosphere.pact.example.relational.contracts.tpch9;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class PartsuppMap extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {
	
	private static Logger LOGGER = Logger.getLogger(PartsuppMap.class);

	/**
	 * Project "partsupp".
	 * 
	 * Output Schema:
	 *  Key: partkey
	 *  Value: (suppkey, supplycost)
	 *
	 */
	@Override
	public void map(PactInteger partKey, Tuple inputTuple,
			Collector<PactInteger, Tuple> output) {
		
		try {
			/* Project (partkey, suppkey, availqty, supplycost, comment) to (suppkey, supplycost): */
			inputTuple.project((0 << 0) | (1 << 1) | (0 << 2) | (1 << 3) | (0 << 4));
			output.collect(partKey, inputTuple);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}
	}

}
