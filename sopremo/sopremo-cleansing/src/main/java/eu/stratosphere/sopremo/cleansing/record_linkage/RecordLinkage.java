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
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class RecordLinkage extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	private ComparativeExpression similarityCondition;

	private Partitioning algorithm;

	private Map<Operator.Output, EvaluationExpression> idProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>();

	private EvaluationExpression duplicateProjection;

	public RecordLinkage(Partitioning algorithm, EvaluationExpression similarityExpression, double threshold,
			JsonStream... inputs) {
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

		List<EvaluationExpression> idProjections = new ArrayList<EvaluationExpression>();
		for (Operator.Output input : getInputs())
			idProjections.add(getIdProjection(input));
		EvaluationExpression duplicateProjection = this.duplicateProjection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0), getIdProjection(0)),
				new PathExpression(new InputSelection(1), getIdProjection(idProjections.size() > 1 ? 1 : 0)));

		SopremoModule algorithmImplementation = this.algorithm.asSopremoOperators(this.similarityCondition,
			this.getInputs(), idProjections, duplicateProjection);
		return algorithmImplementation;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		RecordLinkage other = (RecordLinkage) obj;
		return this.algorithm.equals(other.algorithm) && this.similarityCondition.equals(other.similarityCondition);
	}

	public EvaluationExpression getDuplicateProjection() {
		return this.duplicateProjection;
	}

	public EvaluationExpression getIdProjection(int index) {
		return this.getIdProjection(this.getInput(index));
	}

	public EvaluationExpression getIdProjection(JsonStream input) {
		Output source = input == null ? null : input.getSource();
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

	public void setDuplicateProjection(EvaluationExpression duplicateProjection) {
		if (duplicateProjection == null)
			throw new NullPointerException("duplicateProjection must not be null");

		this.duplicateProjection = duplicateProjection;
	}

	public void setIdProjection(int inputIndex, EvaluationExpression idProjection) {
		this.setIdProjection(this.getInput(inputIndex), idProjection);
	}

	public void setIdProjection(JsonStream input, EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.idProjections.put(input.getSource(), idProjection);
	}

	public abstract static class Partitioning {
		public abstract SopremoModule asSopremoOperators(ComparativeExpression similarityCondition,
				List<Output> inputs, List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);
	}
}
