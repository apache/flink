package eu.stratosphere.pact.iterative.nephele.io;

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class EdgeInput extends DelimitedInputFormat {

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
		String[] ids = new String(bytes).split(",");
		target.setField(0, new PactInteger(Integer.parseInt(ids[0])));
		target.setField(1, new PactInteger(Integer.parseInt(ids[1])));
		return true;
	}

}
