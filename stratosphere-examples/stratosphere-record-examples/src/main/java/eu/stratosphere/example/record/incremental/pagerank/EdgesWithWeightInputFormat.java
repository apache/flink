package eu.stratosphere.example.record.incremental.pagerank;

import eu.stratosphere.api.record.io.TextInputFormat;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactLong;

public class EdgesWithWeightInputFormat extends TextInputFormat {
	
	private static final long serialVersionUID = 1L;
	
	private final PactLong srcId = new PactLong();
	private final PactLong trgId = new PactLong();
	private final PactLong outLinks = new PactLong();
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = str.split("\\s+");

		this.srcId.setValue(Long.parseLong(parts[0]));
		this.trgId.setValue(Long.parseLong(parts[1]));
		this.outLinks.setValue(Long.parseLong(parts[2]));
		
		target.setField(0, this.srcId);
		target.setField(1, this.trgId);
		target.setField(2, this.outLinks);
		return true;
	}

}