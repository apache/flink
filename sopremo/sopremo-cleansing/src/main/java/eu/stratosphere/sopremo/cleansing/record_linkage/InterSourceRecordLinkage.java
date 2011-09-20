package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.scrubbing.Lookup;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

@InputCardinality(min = 2)
public class InterSourceRecordLinkage extends RecordLinkage<InterSourceRecordLinkage> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8586134336913358961L;

	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(this.getName(), this.getInputs().size(), 1);

		final List<RecordLinkageInput> originalInputs = new ArrayList<RecordLinkageInput>();
		for (int index = 0, size = this.getInputs().size(); index < size; index++)
			originalInputs.add(this.getRecordLinkageInput(index));

		final List<RecordLinkageInput> inputs = new ArrayList<RecordLinkageInput>(originalInputs);
		if (this.getLinkageMode().ordinal() >= LinkageMode.TRANSITIVE_LINKS.ordinal()
			&& this.getLinkageMode().getClosureMode().isProvenance())
			for (int index = 0, size = inputs.size(); index < size; index++) {
				inputs.set(index, inputs.get(index).clone());
				inputs.get(index).setResultProjection(inputs.get(index).getIdProjection());
			}
		for (int index = 0, size = inputs.size(); index < size; index++)
			inputs.get(index).setSource(module.getInput(index).getSource());

		Operator<?> duplicatePairs = this.getAlgorithm().getDuplicatePairStream(this.getSimilarityCondition(), inputs);

		if (this.getLinkageMode() == LinkageMode.LINKS_ONLY) {
			module.getOutput(0).setInput(0, duplicatePairs);
			return module;
		}

		Operator<?> output;
		final TransitiveClosure closure = new TransitiveClosure().
			withClosureMode(this.getLinkageMode().getClosureMode()).
			withInputs(duplicatePairs);
		// // already id projected
		// if (recordLinkageInput.getResultProjection() != EvaluationExpression.VALUE)
		// closure.setIdProjection(EvaluationExpression.VALUE);
		output = closure;

		if (this.getLinkageMode().getClosureMode().isProvenance())
			for (int index = 0, size = inputs.size(); index < size; index++)
				if (inputs.get(index).getResultProjection() != originalInputs.get(index).getResultProjection()) {
					Lookup reverseLookup = new Lookup().
						withDictionaryKeyExtraction(originalInputs.get(index).getIdProjection()).
						withDictionaryValueExtraction(originalInputs.get(index).getResultProjection()).
						withInputKeyExtractor(new ArrayAccess(index)).
						withArrayElementsReplacement(true).
						withInputs(output, inputs.get(index));
					output = reverseLookup;
				}

		if (!this.getLinkageMode().isWithSingles()) {
			module.getOutput(0).setInput(0, output);
			return module;
		}

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
		List<Operator<?>> outputs = new ArrayList<Operator<?>>();

		outputs.add(output);

		if (this.getLinkageMode().getClosureMode().isProvenance())
			for (int index = 0; index < originalInputs.size(); index++) {
				ValueSplitter allTuples = new ValueSplitter().
					withInputs(closure).
					withArrayProjection(new ArrayAccess(index)).
					withKeyProjection(new ArrayAccess(0)).
					withValueProjection(EvaluationExpression.NULL);
				RecordLinkageInput recordLinkageInput = originalInputs.get(index);
				Difference singleRecords = new Difference().
					withInputs(module.getInput(index), allTuples).
					withIdentityKey(0, recordLinkageInput.getIdProjection()).
					withValueProjection(0, recordLinkageInput.getResultProjection()).
					withIdentityKey(1, EvaluationExpression.KEY);

				EvaluationExpression[] expressions = new EvaluationExpression[inputs.size()];
				Arrays.fill(expressions, new ArrayCreation());
				expressions[index] = new ArrayCreation(EvaluationExpression.VALUE);
				outputs.add(new Projection().
					withInputs(singleRecords).
					withValueTransformation(new ArrayCreation(expressions)));
			}
		else {
			ValueSplitter allTuples = new ValueSplitter().
				withArrayProjection(EvaluationExpression.VALUE).
				withKeyProjection(new ArrayAccess(0)).
				withValueProjection(EvaluationExpression.NULL).
				withInputs(closure);

			for (int index = 0; index < originalInputs.size(); index++) {
				RecordLinkageInput recordLinkageInput = originalInputs.get(index);
				Difference singleRecords = new Difference().
					withInputs(module.getInput(index), allTuples).
					withIdentityKey(0, recordLinkageInput.getResultProjection()).
					withValueProjection(0, recordLinkageInput.getResultProjection()).
					withIdentityKey(1, EvaluationExpression.KEY);
				outputs.add(new Projection().
					withInputs(singleRecords).
					withValueTransformation(new ArrayCreation(EvaluationExpression.VALUE)));
			}
		}

		module.getOutput(0).setInput(0, new UnionAll().withInputs(outputs));
		return module;
	}

}
