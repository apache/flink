package eu.stratosphere.pact.programs.inputs;

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class TwitterLinkInput extends DelimitedInputFormat {

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
		String[] triple = new String(bytes).split("\t");
		target.setField(1, new PactLong(Long.parseLong(triple[0])));
		target.setField(0, new PactLong(Long.parseLong(triple[1])));
		return true;
	}

}
