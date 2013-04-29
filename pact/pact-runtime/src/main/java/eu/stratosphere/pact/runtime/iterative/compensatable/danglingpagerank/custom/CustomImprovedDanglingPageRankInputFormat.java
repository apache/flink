package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.generic.io.DelimitedInputFormat;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.AsciiLongArrayView;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRankAndDangling;

public class CustomImprovedDanglingPageRankInputFormat extends DelimitedInputFormat<VertexWithRankAndDangling> {

	private AsciiLongArrayView arrayView = new AsciiLongArrayView();

	private static final long DANGLING_MARKER = 1l;
	
	private double initialRank;

	@Override
	public void configure(Configuration parameters) {
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		initialRank = 1.0 / numVertices;
		super.configure(parameters);
	}

	@Override
	public boolean readRecord(VertexWithRankAndDangling target, byte[] bytes, int offset, int numBytes) {

		arrayView.set(bytes, offset, numBytes);

		try {
			arrayView.next();
			target.setVertexID(arrayView.element());

			if (arrayView.next()) {
				target.setDangling(arrayView.element() == DANGLING_MARKER);
			} else {
				target.setDangling(false);
			}

		} catch (NumberFormatException e) {
			throw new RuntimeException("Error parsing " + arrayView.toString(), e);
		}

		target.setRank(initialRank);

		return true;
	}
}
