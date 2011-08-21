package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Arrays;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.scrubbing.Lookup;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class IntraSourceRecordLinkage extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	private final ComparativeExpression similarityCondition;

	private final RecordLinkageAlgorithm algorithm;

	private final RecordLinkageInput recordLinkageInput = new RecordLinkageInput(this, 0);

	private LinkageMode linkageMode = LinkageMode.LINKS_ONLY;

	public LinkageMode getLinkageMode() {
		return this.linkageMode;
	}

	public void setLinkageMode(LinkageMode linkageMode) {
		if (linkageMode == null)
			throw new NullPointerException("linkageMode must not be null");

		this.linkageMode = linkageMode;
	}

	public IntraSourceRecordLinkage(final RecordLinkageAlgorithm algorithm,
			final EvaluationExpression similarityExpression,
			final double threshold, final JsonStream input) {
		super(1, input);
		if (algorithm == null)
			throw new NullPointerException();
		this.algorithm = algorithm;
		this.similarityCondition = new ComparativeExpression(similarityExpression, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(threshold));
	}

	@Override
	public SopremoModule asElementaryOperators() {
		RecordLinkageInput recordLinkageInput = this.recordLinkageInput;
		if (linkageMode.ordinal() >= LinkageMode.TRANSITIVE_LINKS.ordinal())
			recordLinkageInput = recordLinkageInput.minimizeResultOverhead();

		Operator duplicatePairs;
		if (algorithm instanceof IntraSourceRecordLinkageAlgorithm)
			duplicatePairs = ((IntraSourceRecordLinkageAlgorithm) this.algorithm).getIntraSource(
				this.similarityCondition, recordLinkageInput);
		else
			duplicatePairs = simulateIntraSource();

		if (linkageMode == LinkageMode.LINKS_ONLY)
			return SopremoModule.valueOf(getName(), duplicatePairs);

		Operator output;
		final TransitiveClosure closure = new TransitiveClosure(duplicatePairs);
		final boolean cluster = linkageMode != LinkageMode.TRANSITIVE_LINKS;
		closure.setCluster(cluster);
		output = closure;

		if (recordLinkageInput != this.recordLinkageInput) {
			Lookup reverseLookup = new Lookup(closure, recordLinkageInput.getLookupDictionary());
			reverseLookup.setArrayElementsReplacement(cluster);
			output = reverseLookup;
		}

		if (linkageMode != LinkageMode.ALL_CLUSTERS)
			return SopremoModule.valueOf(getName(), output);

		Operator singleRecords = new Difference(this.recordLinkageInput,
			new ValueSplitter(closure).
				withArrayProjection(EvaluationExpression.VALUE).
				withKeyProjection(this.recordLinkageInput.getResultProjection()));

		final Projection wrappedInArray = new Projection(new ArrayCreation(EvaluationExpression.VALUE), singleRecords);
		return SopremoModule.valueOf(getName(), new Union(wrappedInArray, output));
	}

	private Operator simulateIntraSource() {
		// simulate with record linkage
		RecordLinkageInput recordLinkageInput = this.recordLinkageInput.clone();

		EvaluationExpression resultProjection = recordLinkageInput.getResultProjection(), idProjection = EvaluationExpression.VALUE;
		if (resultProjection != EvaluationExpression.VALUE &&
			!recordLinkageInput.getResultProjection().equals(recordLinkageInput.getIdProjection())) {
			if (recordLinkageInput.getIdProjection() == EvaluationExpression.VALUE)
				recordLinkageInput.setResultProjection(EvaluationExpression.VALUE);
			else {
				recordLinkageInput.setResultProjection(new ArrayCreation(recordLinkageInput.getIdProjection(),
					recordLinkageInput.getResultProjection()));
				idProjection = new ArrayAccess(0);
				resultProjection = new ArrayAccess(1);
			}
		}
		Operator allPairs = this.algorithm.getDuplicatePairStream(this.similarityCondition,
			Arrays.asList(recordLinkageInput, recordLinkageInput));
		// remove symmetric and reflexive pairs
		Operator orderedPairs = new Selection(new ComparativeExpression(new PathExpression(new ArrayAccess(0),
			idProjection), BinaryOperator.LESS, new PathExpression(new ArrayAccess(1), idProjection)),
			allPairs);
		return new Projection(resultProjection, orderedPairs);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final IntraSourceRecordLinkage other = (IntraSourceRecordLinkage) obj;

		return this.linkageMode == other.linkageMode &&
			this.algorithm.equals(other.algorithm) && this.similarityCondition.equals(other.similarityCondition)
			&& this.recordLinkageInput.equals(other.recordLinkageInput);
	}

	public RecordLinkageInput getRecordLinkageInput() {
		return this.recordLinkageInput;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.linkageMode.hashCode();
		result = prime * result + this.algorithm.hashCode();
		result = prime * result + this.similarityCondition.hashCode();
		result = prime * result + this.recordLinkageInput.hashCode();
		return result;
	}

}
