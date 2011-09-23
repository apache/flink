package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Arrays;

import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.ArraySplit;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Replace;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;

@InputCardinality(min = 1, max = 1)
public class IntraSourceRecordLinkage extends RecordLinkage<IntraSourceRecordLinkage> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	public RecordLinkageInput getRecordLinkageInput() {
		return super.getRecordLinkageInput(0);
	}

	@SuppressWarnings("cast")
	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(this.getName(), 1, 1);

		RecordLinkageInput recordLinkageInput = this.getRecordLinkageInput(0).clone();
		recordLinkageInput.setSource(module.getInput(0).getSource());
		EvaluationExpression resultProjection = this.getRecordLinkageInput(0).getResultProjection();
		if (this.getLinkageMode().ordinal() >= LinkageMode.TRANSITIVE_LINKS.ordinal() &&
			!recordLinkageInput.getResultProjection().equals(recordLinkageInput.getIdProjection()))
			recordLinkageInput.setResultProjection(recordLinkageInput.getIdProjection());

		Operator<?> duplicatePairs;
		if (this.getAlgorithm() instanceof IntraSourceRecordLinkageAlgorithm)
			duplicatePairs = ((IntraSourceRecordLinkageAlgorithm) this.getAlgorithm()).getIntraSource(
				this.getSimilarityCondition(), recordLinkageInput);
		else
			duplicatePairs = this.simulateIntraSource();

		if (this.getLinkageMode() == LinkageMode.LINKS_ONLY) {
			module.getOutput(0).setInput(0, duplicatePairs);
			return module;
		}

		ClosureMode closureMode = this.getLinkageMode().getClosureMode();
		if (closureMode.isCluster())
			closureMode = ClosureMode.CLUSTER;
		TransitiveClosure closure = new TransitiveClosure().
			withClosureMode(closureMode).
			withInputs(duplicatePairs);
		Operator<?> output = closure;
		// // already id projected
		// if (recordLinkageInput.getResultProjection() != EvaluationExpression.VALUE)
		// closure.setIdProjection(EvaluationExpression.VALUE);

		if (recordLinkageInput.getResultProjection() != resultProjection) {
			Replace reverseLookup = new Replace().
				withDictionaryKeyExtraction(this.getRecordLinkageInput(0).getIdProjection()).
				withDictionaryValueExtraction(this.getRecordLinkageInput(0).getResultProjection()).
				withArrayElementsReplacement(true).
				withInputs(output, module.getInput(0));
			output = reverseLookup;
		}

		if (!this.getLinkageMode().isWithSingles()) {
			module.getOutput(0).setInput(0, output);
			return module;
		}

		ArraySplit allTuples = new ArraySplit().
			withInputs(closure).
			withArrayPath(EvaluationExpression.VALUE).
			withKeyProjection(new ArrayAccess(0)).
			withValueProjection(EvaluationExpression.NULL);
		allTuples.setName("all tuples");
		Difference singleRecords = new Difference().
			withInputs(module.getInput(0), allTuples).
			withIdentityKey(0, this.getRecordLinkageInput(0).getIdProjection()).
			withValueProjection(0, this.getRecordLinkageInput(0).getResultProjection()).
			withIdentityKey(1, EvaluationExpression.KEY);
		singleRecords.setName("singleRecords");

		final Projection wrappedInArray = new Projection().
			withValueTransformation(new ArrayCreation(EvaluationExpression.VALUE)).
			withInputs(singleRecords);

		module.getOutput(0).setInput(0, new UnionAll().withInputs(wrappedInArray, output));
		return module;
	}

	private Operator<?> simulateIntraSource() {
		// simulate with record linkage
		RecordLinkageInput recordLinkageInput = this.getRecordLinkageInput(0).clone();

		EvaluationExpression resultProjection = recordLinkageInput.getResultProjection(), idProjection = EvaluationExpression.VALUE;
		if (resultProjection != EvaluationExpression.VALUE &&
			!recordLinkageInput.getResultProjection().equals(recordLinkageInput.getIdProjection()))
			if (recordLinkageInput.getIdProjection() == EvaluationExpression.VALUE)
				recordLinkageInput.setResultProjection(EvaluationExpression.VALUE);
			else {
				recordLinkageInput.setResultProjection(new ArrayCreation(recordLinkageInput.getIdProjection(),
					recordLinkageInput.getResultProjection()));
				idProjection = new ArrayAccess(0);
				resultProjection = new ArrayAccess(1);
			}
		Operator<?> allPairs = this.getAlgorithm().getDuplicatePairStream(this.getSimilarityCondition(),
			Arrays.asList(recordLinkageInput, recordLinkageInput));
		// remove symmetric and reflexive pairs
		Selection orderedPairs = new Selection().withInputs(allPairs).
			withCondition(new ComparativeExpression(
				new PathExpression(new ArrayAccess(0), idProjection),
				BinaryOperator.LESS,
				new PathExpression(new ArrayAccess(1), idProjection)));
		return new Projection().withValueTransformation(resultProjection).withInputs(orderedPairs);
	}

}
