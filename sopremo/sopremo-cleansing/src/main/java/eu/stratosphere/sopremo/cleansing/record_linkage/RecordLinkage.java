package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.IdentityHashMap;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.InputSelection;

public abstract class RecordLinkage<Self extends RecordLinkage<Self>> extends CompositeOperator<Self> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4477302065457442491L;

	private BooleanExpression duplicateCondition = new ComparativeExpression(new InputSelection(0),
		BinaryOperator.EQUAL, new InputSelection(1));

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

		return this.linkageMode == other.linkageMode && this.algorithm.equals(other.algorithm)
			&& this.duplicateCondition.equals(other.duplicateCondition)
			&& this.recordLinkageInputs.equals(other.recordLinkageInputs);
	}

	public RecordLinkageAlgorithm getAlgorithm() {
		return this.algorithm;
	}

	public LinkageMode getLinkageMode() {
		return this.linkageMode;
	}

	public BooleanExpression getDuplicateCondition() {
		return this.duplicateCondition;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.linkageMode.hashCode();
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.duplicateCondition.hashCode();
		result = prime * result + this.recordLinkageInputs.hashCode();
		return result;
	}

	public void setAlgorithm(RecordLinkageAlgorithm algorithm) {
		if (algorithm == null)
			throw new NullPointerException("algorithm must not be null");

		this.algorithm = algorithm;
	}

	public void setDuplicateCondition(BooleanExpression duplicateCondition) {
		if (duplicateCondition == null)
			throw new NullPointerException("duplicateCondition must not be null");

		this.duplicateCondition = duplicateCondition;
	}

	public Self withDuplicateCondition(BooleanExpression duplicateCondition) {
		this.setDuplicateCondition(duplicateCondition);
		return self();
	}

	public void setLinkageMode(LinkageMode linkageMode) {
		if (linkageMode == null)
			throw new NullPointerException("linkageMode must not be null");

		this.linkageMode = linkageMode;
	}

	public Self withAlgorithm(RecordLinkageAlgorithm algorithm) {
		this.setAlgorithm(algorithm);
		return self();
	}

	public Self withLinkageMode(LinkageMode linkageMode) {
		this.setLinkageMode(linkageMode);
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
