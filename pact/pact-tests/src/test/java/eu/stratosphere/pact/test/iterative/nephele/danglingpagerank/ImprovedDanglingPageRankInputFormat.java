package eu.stratosphere.pact.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.test.iterative.nephele.ConfigUtils;

public class ImprovedDanglingPageRankInputFormat extends TextInputFormat {

	private PactLong vertexID = new PactLong();

	private PactDouble initialRank;

	private BooleanValue isDangling = new BooleanValue();

	private AsciiLongArrayView arrayView = new AsciiLongArrayView();

	private static final long DANGLING_MARKER = 1l;

	@Override
	public void configure(Configuration parameters) {
		long numVertices = ConfigUtils.asLong("pageRank.numVertices", parameters);
		initialRank = new PactDouble(1 / (double) numVertices);
		super.configure(parameters);
	}

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

		arrayView.set(bytes, offset, numBytes);

		try {
			arrayView.next();
			vertexID.setValue(arrayView.element());

			if (arrayView.next()) {
				isDangling.set(arrayView.element() == DANGLING_MARKER);
			} else {
				isDangling.set(false);
			}

		} catch (NumberFormatException e) {
			throw new RuntimeException("Error parsing " + arrayView.toString(), e);
		}

		target.clear();
		target.addField(vertexID);
		target.addField(initialRank);
		target.addField(isDangling);

		return true;
	}
}
