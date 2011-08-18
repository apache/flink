package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.WritableEvaluable;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMatch;

public class Lookup extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5213470669940261166L;

	private WritableEvaluable inputKeyExtractor = EvaluationExpression.SAME_VALUE;

	private EvaluationExpression dictionaryKeyExtraction = new ArrayAccess(0);

	// private boolean lookupAll = false;

	public Lookup(final JsonStream input, final JsonStream dictionary) {
		super(input, dictionary);
	}

	public WritableEvaluable getInputKeyExtractor() {
		return this.inputKeyExtractor;
	}

	public void setInputKeyExtractor(WritableEvaluable inputKeyExtract) {
		if (inputKeyExtract == null)
			throw new NullPointerException("inputKeyExtract must not be null");

		this.inputKeyExtractor = inputKeyExtract;
	}

	public EvaluationExpression getDictionaryKeyExtraction() {
		return this.dictionaryKeyExtraction;
	}

	public void setDictionaryKeyExtraction(EvaluationExpression dictionaryKeyExtraction) {
		if (dictionaryKeyExtraction == null)
			throw new NullPointerException("dictionaryKeyExtraction must not be null");

		this.dictionaryKeyExtraction = dictionaryKeyExtraction;
	}

	public Lookup withInputKeyExtractor(WritableEvaluable inputKeyExtract) {
		this.setInputKeyExtractor(inputKeyExtract);
		return this;
	}

	public Lookup withDictionaryKeyExtraction(EvaluationExpression dictionaryKeyExtraction) {
		this.setDictionaryKeyExtraction(dictionaryKeyExtraction);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		final Projection left = new Projection(this.inputKeyExtractor.asExpression(), EvaluationExpression.SAME_VALUE,
			sopremoModule.getInput(0));
		left.setName("InputKeyExtractor");
		final Projection right = new Projection(this.dictionaryKeyExtraction, EvaluationExpression.SAME_VALUE,
			sopremoModule.getInput(1));
		right.setName("DictionaryKeyExtraction");

		sopremoModule.getOutput(0).setInput(0, new ReplaceWithRightInput(this.inputKeyExtractor, left, right));
		return sopremoModule;
	}

	public static class ReplaceWithRightInput extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private WritableEvaluable inputKeyExtractor;

		public ReplaceWithRightInput(final WritableEvaluable inputKeyExtractor, final JsonStream left,
				final JsonStream right) {
			super(left, right);
			this.inputKeyExtractor = inputKeyExtractor;
		}

		public WritableEvaluable getInputKeyExtractor() {
			return this.inputKeyExtractor;
		}

		public static class Implementation extends
				SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private WritableEvaluable inputKeyExtractor;

			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				out.collect(NullNode.getInstance(), this.inputKeyExtractor.set(value1, value2, this.getContext()));
			}
		}
	}
}