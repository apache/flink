package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;

public class TeraInputFormat extends TextInputFormat<TeraKey, TeraValue> {

	@Override
	public boolean readLine(KeyValuePair<TeraKey, TeraValue> pair, byte[] record) {
		
		System.out.println("Record length: " + record.length);
		
		// TODO Auto-generated method stub
		return false;
	}

	
}
