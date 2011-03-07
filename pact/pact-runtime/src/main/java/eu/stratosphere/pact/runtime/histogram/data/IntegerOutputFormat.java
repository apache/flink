package eu.stratosphere.pact.runtime.histogram.data;

import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class IntegerOutputFormat extends TextOutputFormat<PactInteger, PactInteger> {

	@Override
	public byte[] writeLine(KeyValuePair<PactInteger, PactInteger> pair) {
		PactInteger keyInt = pair.getKey();
		PactInteger valueInt = pair.getValue();
		
		String key = keyInt!=null?keyInt.toString():"";
		String value = valueInt!=null?valueInt.toString():"";
		
		return (key + " " + value + "\n").getBytes();
	}

}
