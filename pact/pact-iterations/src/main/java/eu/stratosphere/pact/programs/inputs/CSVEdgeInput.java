package eu.stratosphere.pact.programs.inputs;

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class CSVEdgeInput extends DelimitedInputFormat {
	PactLong node = new PactLong();
	PactLong neighbour = new PactLong();
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
		String[] pair = new String(bytes).split("\t");
		node.setValue(Long.valueOf(pair[1]));
		neighbour.setValue(Long.valueOf(pair[0]));
		
		target.setField(0, node);
		target.setField(1, neighbour);
		return true;
	}

}
