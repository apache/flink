package eu.stratosphere.pact.runtime.histogram.data;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class IntegerInputFormat extends TextInputFormat<PactInteger, PactInteger> {

	@Override
	public boolean readLine(KeyValuePair<PactInteger, PactInteger> pair, byte[] record) {
		String str = new String(record);
		String splits[] = str.split(" ");
		
		if(!splits[0].isEmpty()) {
			pair.setKey(new PactInteger(Integer.parseInt(splits[0])));
		}
		if(!splits[1].isEmpty()) {
			pair.setValue(new PactInteger(Integer.parseInt(splits[1])));
		}
		
		return true;
	}

}
