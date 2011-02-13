package eu.stratosphere.pact.example.relational.contracts.tpch9;


import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;


public class SuppliersJoin extends MatchStub<PactInteger, PactInteger, Tuple, PactInteger, PactString> {
	
	private static Logger LOGGER = Logger.getLogger(SuppliersJoin.class);
	
	/**
	 * Join "nation" and "supplier" by "nationkey".
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: "nation" (name of the nation)
	 *
	 */
	@Override
	public void match(PactInteger partKey, PactInteger suppKey, Tuple nationVal,
			Collector<PactInteger, PactString> output) {
		
		try {
			PactString nationName = new PactString(nationVal.getStringValueAt(1));
			output.collect(suppKey, nationName);
		} catch (final Exception ex) {
			LOGGER.error(ex);
		}

	}

}
