package eu.stratosphere.sopremo.base;

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@InputCardinality(min = 2, max = 2)
@Name(verb = "replace")
public class Replace extends CompositeOperator<Replace> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5213470669940261166L;

	private EvaluationExpression replaceExpression = EvaluationExpression.VALUE;

	public final static EvaluationExpression FILTER_RECORDS = new SingletonExpression("<filter>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8218311569919645735L;

		@Override
		public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
			throw new EvaluationException("Tag expression");
		}

		@Override
		protected Object readResolve() {
			return FILTER_RECORDS;
		}
	};

	private EvaluationExpression dictionaryKeyExtraction = new ArrayAccess(0),
			dictionaryValueExtraction = new ArrayAccess(1),
			defaultExpression = FILTER_RECORDS;

	private boolean arrayElementsReplacement = false;

	public JsonStreamExpression getDictionary() {
		return new JsonStreamExpression(getInput(1));
	}

	public Replace withDictionary(JsonStreamExpression dictionary) {
		setDictionary(dictionary);
		return this;
	}

	@Property
	@Name(noun = "dictionary", preposition = "with")
	public void setDictionary(JsonStreamExpression dictionary) {
		if (dictionary == null)
			throw new NullPointerException("dictionary must not be null");

		this.setInput(1, dictionary.getStream());
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);

		if (this.arrayElementsReplacement) {
			// final ArraySplit arraySplit = new ArraySplit().
			// withArrayPath(this.replaceExpression).
			// withKeyProjection(new ArrayAccess(0)).
			// withValueProjection(new ArrayAccess(1, 2)).
			// withInputs(sopremoModule.getInput(0));
			//
			// final Operator<?> replacedElements = this.defaultExpression == FILTER_RECORDS ?
			// new ElementStrictReplace().withInputs(arraySplit, right) :
			// new ElementReplaceWithDefault().withDefaultExpression(this.defaultExpression).withInputs(arraySplit,
			// right);
			//
			// final AssembleArray arrayDictionary = new AssembleArray().withInputs(replacedElements);
			//
			// final Replace arrayLookup = new Replace();
			// arrayLookup.setInputs(sopremoModule.getInput(0), arrayDictionary);
			// arrayLookup.setReplaceExpression(this.replaceExpression);
			// Selection emptyArrays = new Selection().
			// withCondition(new UnaryExpression(this.replaceExpression, true)).
			// withInputs(sopremoModule.getInput(0));
			// sopremoModule.getOutput(0).setInput(0, new UnionAll().withInputs(arrayLookup, emptyArrays));
		} else {
			ElementaryOperator<?> replaceAtom;
			if (this.defaultExpression == FILTER_RECORDS)
				replaceAtom = new StrictReplace().withInputKeyExtractor(this.replaceExpression);
			else
				replaceAtom = new ReplaceWithDefaultValue().
					withDefaultExpression(this.defaultExpression).
					withInputKeyExtractor(this.replaceExpression);

			replaceAtom.setKeyExpressions(Arrays.asList(
				new PathExpression(new InputSelection(0), getReplaceExpression()),
				new PathExpression(new InputSelection(1), getDictionaryKeyExtraction())));
			sopremoModule.getOutput(0).setInput(0,
				replaceAtom.withInputs(sopremoModule.getInput(0), sopremoModule.getInput(1)));
		}
		return sopremoModule;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Replace other = (Replace) obj;
		return this.arrayElementsReplacement == other.arrayElementsReplacement &&
			this.defaultExpression.equals(other.defaultExpression) &&
			this.dictionaryKeyExtraction.equals(other.dictionaryKeyExtraction) &&
			this.dictionaryValueExtraction.equals(other.dictionaryValueExtraction) &&
			this.replaceExpression.equals(other.replaceExpression);
	}

	public EvaluationExpression getDefaultExpression() {
		return this.defaultExpression;
	}

	public EvaluationExpression getDictionaryKeyExtraction() {
		return this.dictionaryKeyExtraction;
	}

	public EvaluationExpression getDictionaryValueExtraction() {
		return this.dictionaryValueExtraction;
	}

	public EvaluationExpression getReplaceExpression() {
		return this.replaceExpression;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.arrayElementsReplacement ? 1231 : 1237);
		result = prime * result + this.defaultExpression.hashCode();
		result = prime * result + this.dictionaryKeyExtraction.hashCode();
		result = prime * result + this.dictionaryValueExtraction.hashCode();
		result = prime * result + this.replaceExpression.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.Operator#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append(getName());
		if (isArrayElementsReplacement())
			builder.append(" all ");
		getReplaceExpression().toString(builder);
		if (getInput(1) != null) {
			builder.append(" with ");
			getDictionary().toString(builder);
		}
		builder.append(" default ");
		getDefaultExpression().toString(builder);
	}

	public boolean isArrayElementsReplacement() {
		return this.arrayElementsReplacement;
	}

	@Property(flag = true)
	@Name(adjective = "all")
	public void setArrayElementsReplacement(boolean replaceElementsInArray) {
		this.arrayElementsReplacement = replaceElementsInArray;
	}

	@Property
	@Name(noun = "default")
	public void setDefaultExpression(EvaluationExpression defaultExpression) {
		if (defaultExpression == null)
			throw new NullPointerException("defaultExpression must not be null");

		this.defaultExpression = defaultExpression;
	}

	public void setDictionaryKeyExtraction(EvaluationExpression dictionaryKeyExtraction) {
		if (dictionaryKeyExtraction == null)
			throw new NullPointerException("dictionaryKeyExtraction must not be null");

		this.dictionaryKeyExtraction = dictionaryKeyExtraction;
	}

	public void setDictionaryValueExtraction(EvaluationExpression dictionaryValueExtraction) {
		if (dictionaryValueExtraction == null)
			throw new NullPointerException("dictionaryValueExtraction must not be null");

		this.dictionaryValueExtraction = dictionaryValueExtraction;
	}

	@Property()
	@Name(preposition = "on")
	public void setReplaceExpression(EvaluationExpression inputKeyExtract) {
		if (inputKeyExtract == null)
			throw new NullPointerException("inputKeyExtract must not be null");

		this.replaceExpression = inputKeyExtract;
	}

	public Replace withArrayElementsReplacement(boolean replaceArrays) {
		this.setArrayElementsReplacement(replaceArrays);
		return this;
	}

	public Replace withDictionaryKeyExtraction(EvaluationExpression dictionaryKeyExtraction) {
		this.setDictionaryKeyExtraction(dictionaryKeyExtraction);
		return this;
	}

	public Replace withDefaultExpression(EvaluationExpression defaultExpression) {
		this.setDefaultExpression(defaultExpression);
		return this;
	}

	public Replace withDictionaryValueExtraction(EvaluationExpression dictionaryValueExtraction) {
		this.setDictionaryValueExtraction(dictionaryValueExtraction);
		return this;
	}

	public Replace withReplaceExpression(EvaluationExpression inputKeyExtract) {
		this.setReplaceExpression(inputKeyExtract);
		return this;
	}

	//
	// public static class AssembleArray extends ElementaryOperator<AssembleArray> {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 7334161941683036846L;
	//
	// public static class Implementation extends SopremoReduce {
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode,
	// * eu.stratosphere.sopremo.pact.JsonCollector)
	// */
	// @Override
	// protected void reduce(IArrayNode values, JsonCollector out) {
	// IJsonNode[] array = new IJsonNode[((IArrayNode) key).size()];
	// int replacedCount = 0;
	// for (IJsonNode value : values) {
	// int index = ((NumericNode) ((IArrayNode) value).get(0)).getIntValue();
	// IJsonNode element = ((IArrayNode) value).get(1);
	// array[index] = element;
	// replacedCount++;
	// }
	//
	// // all values replaced
	// if (replacedCount == array.length)
	// out.collect(key, JsonUtil.asArray(array));
	// }
	// }
	// }
	//
	// @InputCardinality(min = 2, max = 2)
	// public static class ElementReplaceWithDefault extends ElementaryOperator<ElementReplaceWithDefault> {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 7334161941683036846L;
	//
	// private EvaluationExpression defaultExpression = FILTER_RECORDS;
	//
	// public void setDefaultExpression(EvaluationExpression defaultExpression) {
	// if (defaultExpression == null)
	// throw new NullPointerException("defaultExpression must not be null");
	//
	// this.defaultExpression = defaultExpression;
	// }
	//
	// public ElementReplaceWithDefault withDefaultExpression(EvaluationExpression defaultExpression) {
	// this.setDefaultExpression(defaultExpression);
	// return this;
	// }
	//
	// public EvaluationExpression getDefaultExpression() {
	// return this.defaultExpression;
	// }
	//
	// public static class Implementation extends SopremoCoGroup {
	//
	// private EvaluationExpression defaultExpression;
	//
	// @Override
	// protected void coGroup(JsonNode key, ArrayNode values1, ArrayNode values2, JsonCollector out) {
	//
	// final Iterator<JsonNode> replaceValueIterator = values2.iterator();
	// JsonNode replaceValue = replaceValueIterator.hasNext() ? replaceValueIterator.next() : null;
	//
	// final Iterator<JsonNode> valueIterator = values1.iterator();
	// final EvaluationContext context = this.getContext();
	// while (valueIterator.hasNext()) {
	// JsonNode value = valueIterator.next();
	// final JsonNode index = ((ArrayNode) value).get(0);
	// JsonNode replacement = replaceValue != null ? replaceValue :
	// this.defaultExpression.evaluate(
	// ((ArrayNode) ((ArrayNode) value).get(1)).get(((IntNode) index).getIntValue()), context);
	// out.collect(((ArrayNode) value).get(1), JsonUtil.asArray(index, replacement));
	// }
	// }
	// }
	// }
	//
	// @InputCardinality(min = 2, max = 2)
	// public static class ElementStrictReplace extends ElementaryOperator<ElementStrictReplace> {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 7334161941683036846L;
	//
	// public static class Implementation extends SopremoMatch {
	// @Override
	// protected void match(final IJsonNode value1, final IJsonNode value2, final JsonCollector out) {
	// out.collect(((IArrayNode) value1).get(1), JsonUtil.asArray(((IArrayNode) value1).get(0), value2));
	// }
	// }
	// }

	@InputCardinality(min = 2, max = 2)
	public static class ReplaceWithDefaultValue extends ElementaryOperator<ReplaceWithDefaultValue> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private EvaluationExpression inputKeyExtractor = EvaluationExpression.VALUE;

		private EvaluationExpression defaultExpression = FILTER_RECORDS;

		public void setInputKeyExtractor(EvaluationExpression inputKeyExtractor) {
			if (inputKeyExtractor == null)
				throw new NullPointerException("inputKeyExtractor must not be null");

			this.inputKeyExtractor = inputKeyExtractor;
		}

		public void setDefaultExpression(EvaluationExpression defaultExpression) {
			if (defaultExpression == null)
				throw new NullPointerException("defaultExpression must not be null");

			this.defaultExpression = defaultExpression;
		}

		public ReplaceWithDefaultValue withInputKeyExtractor(EvaluationExpression prop) {
			this.setInputKeyExtractor(prop);
			return this;
		}

		public ReplaceWithDefaultValue withDefaultExpression(EvaluationExpression prop) {
			this.setDefaultExpression(prop);
			return this;
		}

		public EvaluationExpression getDefaultExpression() {
			return this.defaultExpression;
		}

		public EvaluationExpression getInputKeyExtractor() {
			return this.inputKeyExtractor;
		}

		public static class Implementation extends SopremoCoGroup {
			private EvaluationExpression inputKeyExtractor;

			private EvaluationExpression defaultExpression;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IArrayNode,
			 * eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out) {
				final Iterator<IJsonNode> replaceValueIterator = values2.iterator();
				IJsonNode replaceValue = replaceValueIterator.hasNext() ? replaceValueIterator.next() : null;

				final Iterator<IJsonNode> valueIterator = values1.iterator();
				final EvaluationContext context = this.getContext();
				while (valueIterator.hasNext()) {
					IJsonNode value = valueIterator.next();
					IJsonNode replacement = replaceValue != null ? replaceValue :
						this.defaultExpression.evaluate(this.inputKeyExtractor.evaluate(value, context), context);
					out.collect(this.inputKeyExtractor.set(value, replacement, context));
				}
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class StrictReplace extends ElementaryOperator<StrictReplace> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7334161941683036846L;

		private EvaluationExpression inputKeyExtractor;

		public void setInputKeyExtractor(EvaluationExpression inputKeyExtractor) {
			if (inputKeyExtractor == null)
				throw new NullPointerException("inputKeyExtractor must not be null");

			this.inputKeyExtractor = inputKeyExtractor;
		}

		public EvaluationExpression getInputKeyExtractor() {
			return this.inputKeyExtractor;
		}

		public StrictReplace withInputKeyExtractor(EvaluationExpression prop) {
			this.setInputKeyExtractor(prop);
			return this;
		}

		public static class Implementation extends SopremoMatch {
			private EvaluationExpression inputKeyExtractor;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMatch#match(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(IJsonNode value1, IJsonNode value2, JsonCollector out) {
				out.collect(this.inputKeyExtractor.set(value1, value2, this.getContext()));
			}
		}
	}
}