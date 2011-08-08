package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class RecordLinkage extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	private final ComparativeExpression similarityCondition;

	private final Partitioning algorithm;

	private final Map<Operator.Output, EvaluationExpression> idProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>();

	private EvaluationExpression duplicateProjection;

	public RecordLinkage(final Partitioning algorithm, final EvaluationExpression similarityExpression,
			final double threshold,
			final JsonStream... inputs) {
		super(1, inputs);
		if (algorithm == null)
			throw new NullPointerException();
		this.algorithm = algorithm;
		this.similarityCondition = new ComparativeExpression(similarityExpression, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(threshold));
	}

	@Override
	public SopremoModule asElementaryOperators() {
		if (this.getInputs().size() > 2 || this.getInputs().isEmpty())
			throw new UnsupportedOperationException();

		final List<EvaluationExpression> idProjections = new ArrayList<EvaluationExpression>();
		for (final Operator.Output input : this.getInputs())
			idProjections.add(this.getIdProjection(input));
		EvaluationExpression duplicateProjection = this.duplicateProjection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0), this.getIdProjection(0)),
				new PathExpression(new InputSelection(1), this.getIdProjection(idProjections.size() > 1 ? 1 : 0)));

		final SopremoModule algorithmImplementation = this.algorithm.asSopremoOperators(this.similarityCondition,
			this.getInputs(), idProjections, duplicateProjection);
		return algorithmImplementation;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final RecordLinkage other = (RecordLinkage) obj;
		return this.algorithm.equals(other.algorithm) && this.similarityCondition.equals(other.similarityCondition);
	}

	public EvaluationExpression getDuplicateProjection() {
		return this.duplicateProjection;
	}

	public EvaluationExpression getIdProjection(final int index) {
		return this.getIdProjection(this.getInput(index));
	}

	public EvaluationExpression getIdProjection(final JsonStream input) {
		final Output source = input == null ? null : input.getSource();
		EvaluationExpression keyProjection = this.idProjections.get(source);
		if (keyProjection == null)
			keyProjection = EvaluationExpression.SAME_VALUE;
		return keyProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.similarityCondition.hashCode();
		return result;
	}

	public void setDuplicateProjection(final EvaluationExpression duplicateProjection) {
		if (duplicateProjection == null)
			throw new NullPointerException("duplicateProjection must not be null");

		this.duplicateProjection = duplicateProjection;
	}

	public void setIdProjection(final int inputIndex, final EvaluationExpression idProjection) {
		this.setIdProjection(this.getInput(inputIndex), idProjection);
	}

	public void setIdProjection(final JsonStream input, final EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjections.put(input.getSource(), idProjection);
	}

	public abstract static class Partitioning {
		public abstract SopremoModule asSopremoOperators(ComparativeExpression similarityCondition,
				List<Output> inputs, List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);
	}
}
