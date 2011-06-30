package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.SopremoModule;

public class DuplicateDetection extends CompositeOperator {
	private SimilarityMeasure similarityMeasure;

	@Override
	public SopremoModule asElementaryOperators() {
		return null;
	}

}
