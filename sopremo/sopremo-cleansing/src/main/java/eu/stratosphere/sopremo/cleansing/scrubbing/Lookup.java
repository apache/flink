package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.record_linkage.ValueSplitter;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.WritableEvaluable;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoReduce;

public class Lookup extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5213470669940261166L;

	private WritableEvaluable inputKeyExtractor = EvaluationExpression.VALUE;

	public final static EvaluationExpression FILTER_RECORDS = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8218311569919645735L;

		private Object readResolve() {
			return FILTER_RECORDS;
		}

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			throw new EvaluationException("Tag expression");
		}
	};

	private EvaluationExpression dictionaryKeyExtraction = EvaluationExpression.KEY,
			dictionaryValueExtraction = EvaluationExpression.VALUE,
			defaultExpression = FILTER_RECORDS;

	public EvaluationExpression getDefaultExpression() {
		return this.defaultExpression;
	}

	public void setDefaultExpression(EvaluationExpression defaultExpression) {
		if (defaultExpression == null)
			throw new NullPointerException("defaultExpression must not be null");

		this.defaultExpression = defaultExpression;
	}

	private boolean arrayElementsReplacement = false;

	public boolean isArrayElementsReplacement() {
		return this.arrayElementsReplacement;
	}

	public void setArrayElementsReplacement(boolean replaceElementsInArray) {
		this.arrayElementsReplacement = replaceElementsInArray;
	}

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

	public Lookup withArrayElementsReplacement(boolean replaceArrays) {
		this.setArrayElementsReplacement(replaceArrays);
		return this;
	}

	public Lookup withDictionaryKeyExtraction(EvaluationExpression dictionaryKeyExtraction) {
		this.setDictionaryKeyExtraction(dictionaryKeyExtraction);
		return this;
	}

	public EvaluationExpression getDictionaryValueExtraction() {
		return dictionaryValueExtraction;
	}

	public void setDictionaryValueExtraction(EvaluationExpression dictionaryValueExtraction) {
		if (dictionaryValueExtraction == null)
			throw new NullPointerException("dictionaryValueExtraction must not be null");

		this.dictionaryValueExtraction = dictionaryValueExtraction;
	}

	public Lookup withDictionaryValueExtraction(EvaluationExpression dictionaryValueExtraction) {
		this.setDictionaryValueExtraction(dictionaryValueExtraction);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		final Projection right = new Projection(this.dictionaryKeyExtraction, this.dictionaryValueExtraction,
			sopremoModule.getInput(1));

		if (arrayElementsReplacement) {
			final ValueSplitter arraySplit = new ValueSplitter(sopremoModule.getInput(0));
			arraySplit.withArrayProjection(this.inputKeyExtractor.asExpression());
			arraySplit.withKeyProjection(new ArrayAccess(0));
			arraySplit.withValueProjection(new ArrayAccess(1, 2));

			final Operator replacedElements = defaultExpression == FILTER_RECORDS ? new ElementStrictReplace(
				arraySplit, right) : new ElementReplaceWithDefault(defaultExpression, arraySplit, right);
			final Operator arrayDictionary = new UnionAll(new AssembleArray(replacedElements),
				new Projection(EvaluationExpression.VALUE, EvaluationExpression.VALUE,
					new Source(new ArrayCreation(new ArrayCreation()))));

			final Lookup arrayLookup = new Lookup(sopremoModule.getInput(0), arrayDictionary);
			arrayLookup.setInputKeyExtractor(inputKeyExtractor);
			sopremoModule.getOutput(0).setInput(0, arrayLookup);
		} else {
			final Projection left = new Projection(this.inputKeyExtractor.asExpression(),
				EvaluationExpression.VALUE,
				sopremoModule.getInput(0));
			if (defaultExpression == FILTER_RECORDS)
				sopremoModule.getOutput(0).setInput(0, new StrictReplace(this.inputKeyExtractor, left, right));
			else
				sopremoModule.getOutput(0).setInput(0,
					new ReplaceWithDefaultValue(this.inputKeyExtractor, this.defaultExpression, left, right));
		}
		return sopremoModule;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.arrayElementsReplacement ? 1231 : 1237);
		result = prime * result + this.defaultExpression.hashCode();
		result = prime * result + this.dictionaryKeyExtraction.hashCode();
		result = prime * result + this.dictionaryValueExtraction.hashCode();
		result = prime * result + this.inputKeyExtractor.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Lookup other = (Lookup) obj;
		return this.arrayElementsReplacement == other.arrayElementsReplacement &&
			this.defaultExpression.equals(other.defaultExpression) &&
			this.dictionaryKeyExtraction.equals(other.dictionaryKeyExtraction) &&
			this.dictionaryValueExtraction.equals(other.dictionaryValueExtraction) &&
			this.inputKeyExtractor.equals(other.inputKeyExtractor);
	}

	public static class StrictReplace extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private WritableEvaluable inputKeyExtractor;

		public StrictReplace(final WritableEvaluable inputKeyExtractor, final JsonStream left,
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

	public static class ElementReplaceWithDefault extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private final EvaluationExpression defaultExpression;

		public EvaluationExpression getDefaultExpression() {
			return this.defaultExpression;
		}

		public ElementReplaceWithDefault(EvaluationExpression defaultExpression, final JsonStream left,
				final JsonStream right) {
			super(left, right);
			this.defaultExpression = defaultExpression;
		}

		public static class Implementation extends
				SopremoCoGroup<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {

			private EvaluationExpression defaultExpression;

			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				
				
				final Iterator<JsonNode> replaceValueIterator = values2.iterator();
				JsonNode replaceValue = replaceValueIterator.hasNext() ? replaceValueIterator.next() : null;

				final Iterator<JsonNode> valueIterator = values1.iterator();
				final EvaluationContext context = getContext();
				while (valueIterator.hasNext()) {
					JsonNode value = valueIterator.next();
					final JsonNode index = value.get(0);
					JsonNode replacement = replaceValue != null ? replaceValue :
						defaultExpression.evaluate(value.get(1).get(index.getIntValue()), context);
					out.collect(value.get(1), JsonUtil.asArray(index, replacement));
				}
			}
		}
	}

	public static class ElementStrictReplace extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		public ElementStrictReplace(final JsonStream left, final JsonStream right) {
			super(left, right);
		}

		public static class Implementation extends
				SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				out.collect(value1.get(1), JsonUtil.asArray(value1.get(0), value2));
			}
		}
	}

	public static class AssembleArray extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		public AssembleArray(final JsonStream input) {
			super(input);
		}

		public static class Implementation extends
				SopremoReduce<Key, PactJsonObject, Key, PactJsonObject> {
			@Override
			protected void reduce(JsonNode key, StreamArrayNode values, JsonCollector out) {
				JsonNode[] array = new JsonNode[key.size()];
				int replacedCount = 0;
				for (JsonNode value : values) {
					int index = value.get(0).getIntValue();
					JsonNode element = value.get(1);
					array[index] = element;
					replacedCount++;
				}

				// all values replaced
				if (replacedCount == array.length)
					out.collect(key, JsonUtil.asArray(array));
			}
		}
	}

	public static class ReplaceWithDefaultValue extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private WritableEvaluable inputKeyExtractor;

		private EvaluationExpression defaultExpression;

		public ReplaceWithDefaultValue(final WritableEvaluable inputKeyExtractor,
				EvaluationExpression defaultExpression, final JsonStream left, final JsonStream right) {
			super(left, right);
			this.inputKeyExtractor = inputKeyExtractor;
			this.defaultExpression = defaultExpression;
		}

		public WritableEvaluable getInputKeyExtractor() {
			return this.inputKeyExtractor;
		}

		public EvaluationExpression getDefaultExpression() {
			return this.defaultExpression;
		}

		public static class Implementation extends
				SopremoCoGroup<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private WritableEvaluable inputKeyExtractor;

			private EvaluationExpression defaultExpression;

			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				final Iterator<JsonNode> replaceValueIterator = values2.iterator();
				JsonNode replaceValue = replaceValueIterator.hasNext() ? replaceValueIterator.next() : null;

				final Iterator<JsonNode> valueIterator = values1.iterator();
				final EvaluationContext context = getContext();
				while (valueIterator.hasNext()) {
					JsonNode value = valueIterator.next();
					JsonNode replacement = replaceValue != null ? replaceValue :
						defaultExpression.evaluate(
							((EvaluationExpression) this.inputKeyExtractor).evaluate(value, context), context);
					out.collect(NullNode.getInstance(), this.inputKeyExtractor.set(value, replacement, context));
				}
			}
		}
	}
}