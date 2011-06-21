package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;

public class TeraInputFormat extends TextInputFormat<TeraKey, TeraValue> {

	@Override
	public boolean readLine(KeyValuePair<TeraKey, TeraValue> pair, byte[] record) {
		
		if(record.length != (TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE)) {
			return false;
		}
		
		final TeraKey key = new TeraKey(record);
		final TeraValue value = new TeraValue(record);
		pair.setKey(key);
		pair.setValue(value);
		
		System.out.println("VALUE: " + value);
		
		return true;
	}

	
}
