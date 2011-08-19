package eu.stratosphere.sopremo.cleansing.record_linkage;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonStreamContext;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.scrubbing.Lookup;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
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

	private final RecordLinkageAlgorithm algorithm;

	private final Map<Operator.Output, RecordLinkageInput> recordLinkageInputs = new IdentityHashMap<Operator.Output, RecordLinkageInput>();

	private boolean clusterMode;

	public RecordLinkage(final RecordLinkageAlgorithm algorithm, final EvaluationExpression similarityExpression,
			final double threshold, final JsonStream... inputs) {
		super(1, inputs);
		if (algorithm == null)
			throw new NullPointerException();
		this.algorithm = algorithm;
		this.similarityCondition = new ComparativeExpression(similarityExpression, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(threshold));
	}

	@Override
	public SopremoModule asElementaryOperators() {

		final List<RecordLinkageInput> inputs = new ArrayList<RecordLinkageInput>();
		for (int index = 0, size = this.getInputs().size(); index < size; index++)
			inputs.add(this.getRecordLinkageInput(index));

		Int2ObjectMap<EvaluationExpression> resubstituteExpressions = new Int2ObjectOpenHashMap<EvaluationExpression>();
		if (clusterMode) {
			for (int index = 0, size = inputs.size(); index < size; index++) {
				RecordLinkageInput input = inputs.get(index);
				if (input.getIdProjection() != EvaluationExpression.SAME_VALUE
					&& !input.getResultProjection().equals(input.getIdProjection())) {
					resubstituteExpressions.put(index, input.getResultProjection());
					input = input.clone();
					input.setResultProjection(input.getIdProjection());
					inputs.set(index, input);
				}
			}
		}
		Operator duplicatePairs = this.algorithm.getDuplicatePairStream(this.similarityCondition, inputs);

		if (!clusterMode)
			return SopremoModule.valueOf(getName(), duplicatePairs);

		if (inputs.size() == 1) {
			// special case intra source
			Operator clusters = new TransitiveClosure(duplicatePairs);
			Operator singleRecords = new Difference(this.getInputs().get(0),
				new Projection(new ArrayAccess(0), clusters),
				new Projection(new ArrayAccess(1), clusters)).
				withKeyProjection(0, inputs.get(0).getIdProjection());

			if (!resubstituteExpressions.isEmpty()) {
				Operator id2ResultList = new Projection(inputs.get(0).getIdProjection(),
					resubstituteExpressions.get(0), inputs.get(0));

				clusters = new Lookup(clusters, id2ResultList).withInputKeyExtractor(new ArrayAccess());
				singleRecords = new Lookup(singleRecords, id2ResultList).withInputKeyExtractor(new ArrayAccess(0));
			}

			return SopremoModule.valueOf(getName(), new Union(singleRecords, clusters));
		}

		Operator closure = new TransitiveClosure(duplicatePairs);
		List<Operator> singleExtractors = new ArrayList<Operator>();
		for (int index = 0; index < inputs.size(); index++) {
			EvaluationExpression[] singleArray = new EvaluationExpression[inputs.size()];
			Arrays.fill(singleArray, EvaluationExpression.NULL);

			singleArray[index] = inputs.get(index).getResultProjection();
			Difference singles = new Difference(this.getInputs().get(0),
				new Projection(new ArrayAccess(index), closure)).
				withKeyProjection(0, inputs.get(0).getIdProjection()).
				withValueProjection(new ArrayCreation(singleArray));
			singleExtractors.add(singles);
		}

		Operator clusters = closure;
		for (Entry<Integer, EvaluationExpression> resubstituteExpression : resubstituteExpressions.entrySet()) {
			Integer inputIndex = resubstituteExpression.getKey();
			Operator id2ResultList = new Projection(inputs.get(inputIndex).getIdProjection(),
				resubstituteExpression.getValue(), inputs.get(inputIndex));
			clusters = new Lookup(clusters, id2ResultList).withInputKeyExtractor(
				new PathExpression.Writable(new ArrayAccess(inputIndex), new ArrayAccess()));
		}
		singleExtractors.add(clusters);

		return SopremoModule.valueOf(getName(), new Union(singleExtractors));
	}

	public class RecordLinkageInput implements JsonStream, Cloneable {
		private final int index;

		private EvaluationExpression idProjection = EvaluationExpression.SAME_VALUE;

		private EvaluationExpression resultProjection = EvaluationExpression.SAME_VALUE;

		private RecordLinkageInput(int index) {
			this.index = index;
		}

		@Override
		protected RecordLinkageInput clone() {
			try {
				return (RecordLinkageInput) super.clone();
			} catch (CloneNotSupportedException e) {
				// cannot happen
				return null;
			}
		}

		@Override
		public Output getSource() {
			return getInput(index);
		}

		public EvaluationExpression getIdProjection() {
			return idProjection;
		}

		public void setIdProjection(EvaluationExpression idProjection) {
			if (idProjection == null)
				throw new NullPointerException("idProjection must not be null");

			this.idProjection = idProjection;
		}

		public EvaluationExpression getResultProjection() {
			return resultProjection;
		}

		public void setResultProjection(EvaluationExpression resultProjection) {
			if (resultProjection == null)
				throw new NullPointerException("resultProjection must not be null");

			this.resultProjection = resultProjection;
		}

		@Override
		public String toString() {
			return String.format("RecordLinkageInput [index=%s, idProjection=%s, resultProjection=%s]", index,
				idProjection, resultProjection);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + idProjection.hashCode();
			result = prime * result + index;
			result = prime * result + resultProjection.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;

			RecordLinkageInput other = (RecordLinkageInput) obj;
			return index == other.index && idProjection.equals(other.idProjection)
				&& resultProjection.equals(other.resultProjection);
		}

		private RecordLinkage getOuterType() {
			return RecordLinkage.this;
		}

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

		return this.clusterMode == other.clusterMode &&
			this.algorithm.equals(other.algorithm) && this.similarityCondition.equals(other.similarityCondition)
			&& this.recordLinkageInputs.equals(other.recordLinkageInputs);
	}

	public RecordLinkageInput getRecordLinkageInput(final int index) {
		RecordLinkageInput recordLinkageInput = this.recordLinkageInputs.get(getInput(index));
		if (recordLinkageInput == null)
			this.recordLinkageInputs.put(getInput(index), recordLinkageInput = new RecordLinkageInput(index));
		return recordLinkageInput;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.clusterMode ? 1337 : 1237);
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.similarityCondition.hashCode();
		result = prime * result + this.recordLinkageInputs.hashCode();
		return result;
	}

}
