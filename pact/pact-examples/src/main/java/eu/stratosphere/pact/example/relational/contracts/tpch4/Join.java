/**
 * 
 */
package eu.stratosphere.pact.example.relational.contracts.tpch4;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Implements the equijoin on the orderkey and performs the projection on 
 * the order priority as well.
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class Join extends MatchStub<PactInteger, Tuple, Tuple, PactString, Tuple> {

	private static Logger LOGGER = Logger.getLogger(Join.class);
	
	@Override
	public void match(PactInteger key, Tuple orderValue, Tuple lineValue,
			Collector<PactString, Tuple> outputTuple) {
		
		try{
		
			LOGGER.info("before projection: " + orderValue + " on key " + key);
			orderValue.project(32);
			LOGGER.info("after projection: " + orderValue + " on key " + key);
			String newOrderKey = orderValue.getStringValueAt(0);
		
			outputTuple.collect(new PactString(newOrderKey), orderValue);
		}catch(Exception ex)
		{
			LOGGER.error(ex);
		}
	}

}
