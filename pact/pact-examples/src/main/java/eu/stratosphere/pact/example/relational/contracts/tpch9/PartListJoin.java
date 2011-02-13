package eu.stratosphere.pact.example.relational.contracts.tpch9;


import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.types.tpch9.*;


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
