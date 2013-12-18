package eu.stratosphere.test.iterative.nephele.customdanglingpagerank;

import eu.stratosphere.api.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.iterative.nephele.ConfigUtils;
import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRankAndDangling;
import eu.stratosphere.test.iterative.nephele.danglingpagerank.AsciiLongArrayView;

public class CustomImprovedDanglingPageRankInputFormat extends DelimitedInputFormat<VertexWithRankAndDangling> {
	private static final long serialVersionUID = 1L;

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
