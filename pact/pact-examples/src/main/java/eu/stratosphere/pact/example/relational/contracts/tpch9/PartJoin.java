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
			IntPair newKey = new IntPair(partKey, new PactInteger(Integer.parseInt(partSuppValue.getStringValueAt(1))));
			String supplyCost = partSuppValue.getStringValueAt(0);
			LOGGER.info("new Parts key: (" +  partKey + ", " + partSuppValue.getLongValueAt(1) + ")");
			LOGGER.info("Parts supplycost: " + supplyCost);
		
			output.collect(newKey, new PactString(supplyCost));
		} catch(Exception e) {
			LOGGER.error(e);
		}
	}

}
