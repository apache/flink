package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;

public class TeraOutputFormat extends TextOutputFormat<TeraKey, TeraValue> {

	private final byte[] buffer = new byte[TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE];
	
	@Override
	public byte[] writeLine(KeyValuePair<TeraKey, TeraValue> pair) {
		
		pair.getKey().copyToBuffer(this.buffer);
		pair.getValue().copyToBuffer(this.buffer);
		
		return this.buffer;
	}
	
}
