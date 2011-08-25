package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.scrubbing.Lookup;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class InterSourceRecordLinkage extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	private final ComparativeExpression similarityCondition;

	private final RecordLinkageAlgorithm algorithm;

	private final Map<Operator.Output, RecordLinkageInput> recordLinkageInputs = new IdentityHashMap<Operator.Output, RecordLinkageInput>();

	private LinkageMode linkageMode = LinkageMode.LINKS_ONLY;

	public InterSourceRecordLinkage(final RecordLinkageAlgorithm algorithm,
			final EvaluationExpression similarityExpression,
			final double threshold, final JsonStream... inputs) {
		this(algorithm, similarityExpression, threshold, Arrays.asList(inputs));
	}
	
	public InterSourceRecordLinkage(final RecordLinkageAlgorithm algorithm,
			final EvaluationExpression similarityExpression,
			final double threshold, final List<? extends JsonStream> inputs) {
		super(1, inputs);
		if (algorithm == null)
			throw new NullPointerException();
		this.algorithm = algorithm;
		this.similarityCondition = new ComparativeExpression(similarityExpression, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(threshold));
	}

	@Override
	public SopremoModule asElementaryOperators() {

		final List<RecordLinkageInput> originalInputs = new ArrayList<RecordLinkageInput>();
		for (int index = 0, size = this.getInputs().size(); index < size; index++)
			originalInputs.add(this.getRecordLinkageInput(index));

		final List<RecordLinkageInput> inputs = new ArrayList<RecordLinkageInput>(originalInputs);
		if (this.linkageMode.ordinal() >= LinkageMode.TRANSITIVE_LINKS.ordinal()
			&& this.linkageMode.getClosureMode().isProvenance())
			for (int index = 0, size = inputs.size(); index < size; index++) {
				inputs.set(index, inputs.get(index).clone());
				inputs.get(index).setResultProjection(inputs.get(index).getIdProjection());
			}

		Operator duplicatePairs = this.algorithm.getDuplicatePairStream(this.similarityCondition, inputs);

		if (this.linkageMode == LinkageMode.LINKS_ONLY)
			return SopremoModule.valueOf(this.getName(), duplicatePairs);

		Operator output;
		final TransitiveClosure closure = new TransitiveClosure(duplicatePairs);
		closure.setClosureMode(this.linkageMode.getClosureMode());
		// // already id projected
		// if (recordLinkageInput.getResultProjection() != EvaluationExpression.VALUE)
		// closure.setIdProjection(EvaluationExpression.VALUE);
		output = closure;

		if (this.linkageMode.getClosureMode().isProvenance())
			for (int index = 0, size = inputs.size(); index < size; index++)
				if (inputs.get(index).getResultProjection() != originalInputs.get(index).getResultProjection()) {
					Lookup reverseLookup = new Lookup(output, originalInputs.get(index).getLookupDictionary());
					reverseLookup.withInputKeyExtractor(new ArrayAccess(index));
					reverseLookup.setArrayElementsReplacement(true);
					output = reverseLookup;
				}

		if (!this.linkageMode.isWithSingles())
			return SopremoModule.valueOf(this.getName(), output);

		// List<Operator> singleExtractors = new ArrayList<Operator>();
		// for (int index = 0; index < originalInputs.size(); index++) {
		// EvaluationExpression[] singleArray = new EvaluationExpression[originalInputs.size()];
		// Arrays.fill(singleArray, EvaluationExpression.NULL);
		//
		// singleArray[index] = originalInputs.get(index).getResultProjection();
		// Difference singles = new Difference(this.getInputs().get(0),
		// new Projection(new ArrayAccess(index), closure)).
		// withKeyProjection(0, originalInputs.get(0).getIdProjection()).
		// withValueProjection(new ArrayCreation(singleArray));
		// singleExtractors.add(singles);
		// }
		//
		// Operator clusters = closure;
		// for (Entry<Integer, EvaluationExpression> resubstituteExpression : resubstituteExpressions.entrySet()) {
		// Integer inputIndex = resubstituteExpression.getKey();
		// Operator id2ResultList = new Projection(originalInputs.get(inputIndex).getIdProjection(),
		// resubstituteExpression.getValue(), originalInputs.get(inputIndex));
		// clusters = new Lookup(clusters, id2ResultList).withInputKeyExtractor(
		// new PathExpression.Writable(new ArrayAccess(inputIndex), new ArrayAccess()));
		// }
		// singleExtractors.add(clusters);
		List<Operator> outputs = new ArrayList<Operator>();

		if (this.linkageMode.getClosureMode().isProvenance())
			for (int index = 0; index < originalInputs.size(); index++) {
				ValueSplitter allTuples = new ValueSplitter(closure).
					withArrayProjection(new ArrayAccess(index)).
					withKeyProjection(new ArrayAccess(0)).
					withValueProjection(EvaluationExpression.NULL);
				RecordLinkageInput recordLinkageInput = originalInputs.get(index);
				Operator singleRecords = new Difference(recordLinkageInput, allTuples).
					withKeyProjection(0, recordLinkageInput.getIdProjection()).
					withValueProjection(0, recordLinkageInput.getResultProjection()).
					withKeyProjection(1, EvaluationExpression.KEY);

				EvaluationExpression[] expressions = new EvaluationExpression[inputs.size()];
				Arrays.fill(expressions, new ArrayCreation());
				expressions[index] = new ArrayCreation(EvaluationExpression.VALUE);
				final Projection wrappedInArray = new Projection(new ArrayCreation(expressions), singleRecords);
				outputs.add(wrappedInArray);
			}
		else {
			ValueSplitter allTuples = new ValueSplitter(closure).
				withArrayProjection(EvaluationExpression.VALUE).
				withKeyProjection(new ArrayAccess(0)).
				withValueProjection(EvaluationExpression.NULL);

			for (int index = 0; index < originalInputs.size(); index++) {
				RecordLinkageInput recordLinkageInput = originalInputs.get(index);
				Operator singleRecords = new Difference(recordLinkageInput, allTuples).
					withKeyProjection(0, recordLinkageInput.getResultProjection()).
					withValueProjection(0, recordLinkageInput.getResultProjection()).
					withKeyProjection(1, EvaluationExpression.KEY);
				outputs.add(new Projection(new ArrayCreation(EvaluationExpression.VALUE), singleRecords));
			}
		}

		outputs.add(output);

		return SopremoModule.valueOf(this.getName(), new UnionAll(outputs));
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final InterSourceRecordLinkage other = (InterSourceRecordLinkage) obj;

		return this.linkageMode == other.linkageMode &&
			this.algorithm.equals(other.algorithm) && this.similarityCondition.equals(other.similarityCondition)
			&& this.recordLinkageInputs.equals(other.recordLinkageInputs);
	}

	public LinkageMode getLinkageMode() {
		return this.linkageMode;
	}

	public RecordLinkageInput getRecordLinkageInput(final int index) {
		RecordLinkageInput recordLinkageInput = this.recordLinkageInputs.get(this.getInput(index));
		if (recordLinkageInput == null)
			this.recordLinkageInputs
				.put(this.getInput(index), recordLinkageInput = new RecordLinkageInput(this, index));
		return recordLinkageInput;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.linkageMode.hashCode();
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.similarityCondition.hashCode();
		result = prime * result + this.recordLinkageInputs.hashCode();
		return result;
	}

	public void setLinkageMode(LinkageMode linkageMode) {
		if (linkageMode == null)
			throw new NullPointerException("linkageMode must not be null");

		this.linkageMode = linkageMode;
	}

}
