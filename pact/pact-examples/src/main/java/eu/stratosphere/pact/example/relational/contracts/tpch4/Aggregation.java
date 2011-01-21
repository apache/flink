/**
 * 
 */
package eu.stratosphere.pact.example.relational.contracts.tpch4;

import java.util.Iterator;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Implements the count(*) part. 
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
@SameKey
public class Aggregation extends ReduceStub<PactString, Tuple, PactString, Tuple> {

	private static final Logger LOGGER = Logger.getLogger(Aggregation.class);
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public void reduce(PactString key, Iterator<Tuple> values, Collector<PactString, Tuple> out) {
		
		logic(key, values, out, "reduce");
	}

	private void logic(PactString key, Iterator<Tuple> values, Collector<PactString, Tuple> out, String context) {
		long count = 0;
		Tuple t = null;
		while(values.hasNext()) {
		 	t = values.next();
		 	count++;
		}
		
		if(t != null)
		{
			t.addAttribute("" + count);
		}
		
		if(LOGGER.isDebugEnabled())
		{
			LOGGER.debug("Constructed tuple " + t.toString() + " in " + context + "() as result");
		}
		
		out.collect(key, t);
	}
	
	@Override
	public void combine(PactString key, Iterator<Tuple> values,
			Collector<PactString, Tuple> out) {
		logic(key, values, out, "combine");
	}

}
