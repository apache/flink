package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.IdentityHashMap;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class RecordLinkage<Self extends RecordLinkage<Self>> extends CompositeOperator<Self> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4477302065457442491L;

	private EvaluationExpression similarityExpression = new ConstantExpression(1);

	private double threshold = 0;

	private RecordLinkageAlgorithm algorithm = new Naive();

	private final Map<JsonStream, RecordLinkageInput> recordLinkageInputs = new IdentityHashMap<JsonStream, RecordLinkageInput>();

	private LinkageMode linkageMode = LinkageMode.LINKS_ONLY;

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final RecordLinkage<?> other = (RecordLinkage<?>) obj;

		return this.linkageMode == other.linkageMode && this.threshold == other.threshold &&
			this.algorithm.equals(other.algorithm) && this.similarityExpression.equals(other.similarityExpression) &&
			this.recordLinkageInputs.equals(other.recordLinkageInputs);
	}

	public RecordLinkageAlgorithm getAlgorithm() {
		return this.algorithm;
	}

	public LinkageMode getLinkageMode() {
		return this.linkageMode;
	}

	protected ComparativeExpression getSimilarityCondition() {
		return new ComparativeExpression(this.similarityExpression, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(this.threshold));
	}

	public EvaluationExpression getSimilarityExpression() {
		return this.similarityExpression;
	}

	public double getThreshold() {
		return this.threshold;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.linkageMode.hashCode();
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.similarityExpression.hashCode();
		long temp;
		temp = Double.doubleToLongBits(this.threshold);
		result = prime * result + (int) (temp ^ temp >>> 32);
		result = prime * result + this.recordLinkageInputs.hashCode();
		return result;
	}

	public void setAlgorithm(RecordLinkageAlgorithm algorithm) {
		if (algorithm == null)
			throw new NullPointerException("algorithm must not be null");

		this.algorithm = algorithm;
	}

	public void setLinkageMode(LinkageMode linkageMode) {
		if (linkageMode == null)
			throw new NullPointerException("linkageMode must not be null");

		this.linkageMode = linkageMode;
	}

	public void setSimilarityExpression(EvaluationExpression similarityExpression) {
		if (similarityExpression == null)
			throw new NullPointerException("similarityExpression must not be null");

		this.similarityExpression = similarityExpression;
	}

	public void setThreshold(double threshold) {
		if (threshold < 0 || threshold > 1)
			throw new IllegalArgumentException("threshold must be in [0;1]");

		this.threshold = threshold;
	}

	public Self withAlgorithm(RecordLinkageAlgorithm algorithm) {
		this.setAlgorithm(algorithm);
		return self();
	}

	public Self withLinkageMode(LinkageMode linkageMode) {
		this.setLinkageMode(linkageMode);
		return self();
	}

	public Self withSimilarityExpression(EvaluationExpression evaluationExpression) {
		this.setSimilarityExpression(evaluationExpression);
		return self();
	}

	public Self withThreshold(double threshold) {
		this.setThreshold(threshold);
		return self();
	}

	public RecordLinkageInput getRecordLinkageInput(final int index) {
		RecordLinkageInput recordLinkageInput = this.recordLinkageInputs.get(this.getInput(index));
		if (recordLinkageInput == null)
			this.recordLinkageInputs
				.put(this.getInput(index), recordLinkageInput = new RecordLinkageInput(this, index));
		return recordLinkageInput;
	}

}
