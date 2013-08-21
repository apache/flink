package eu.stratosphere.pact.common.contract;

import java.util.Random;

import eu.stratosphere.pact.common.io.GenericTableOutputFormat;
import eu.stratosphere.pact.generic.contract.Contract;


/**
 * A sink for writing to HBase
 */
public class HBaseDataSink extends GenericDataSink
{
	private static final int IDENTIFYIER_LEN = 16;
	
	public HBaseDataSink(Class<? extends GenericTableOutputFormat> c, Contract input, String name)
	{
		super(c, input, name);
		
		// generate a random unique identifier string
		final Random rnd = new Random();
		final StringBuilder bld = new StringBuilder();
		for (int i = 0; i < IDENTIFYIER_LEN; i++) {
			bld.append((char) (rnd.nextInt(26) + 'a'));
		}
		
		setParameter(GenericTableOutputFormat.JT_ID_KEY, bld.toString());
		setParameter(GenericTableOutputFormat.JOB_ID_KEY, rnd.nextInt());
	}

}
