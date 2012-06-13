package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.replace.ArrayReplaceWithDefaultValue;
import eu.stratosphere.sopremo.base.replace.ArrayStrictReplace;
import eu.stratosphere.sopremo.base.replace.ArrayStrictReplace.ElementStrictReplace;
import eu.stratosphere.sopremo.base.replace.ReplaceBase;
import eu.stratosphere.sopremo.base.replace.ReplaceWithDefaultValue;
import eu.stratosphere.sopremo.base.replace.StrictReplace;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NumericNode;

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

	public final static EvaluationExpression KEEP_VALUE = new SingletonExpression("<keep>") {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1652841463219871730L;

		@Override
		public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
			throw new EvaluationException("Tag expression");
		}

		@Override
		protected Object readResolve() {
			return KEEP_VALUE;
		}
	};

	private EvaluationExpression dictionaryKeyExtraction = new ArrayAccess(0),
			dictionaryValueExtraction = new ArrayAccess(1),
			defaultExpression = KEEP_VALUE;

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
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);

		EvaluationExpression defaultExpression =
			this.defaultExpression == KEEP_VALUE ? this.replaceExpression : this.defaultExpression;
		if (this.arrayElementsReplacement) {
			final ArraySplit arraySplit =
				new ArraySplit().
					withArrayPath(this.replaceExpression).
					withSplitProjection(ArraySplit.ResultField.Element, ArraySplit.ResultField.Index,
						ArraySplit.ResultField.Array).
					withInputs(sopremoModule.getInput(0));

			Replace replacedElements = new Replace().
				withInputs(arraySplit, sopremoModule.getInput(1)).
				withDefaultExpression(this.defaultExpression).
				withDictionaryValueExtraction(dictionaryValueExtraction).
				withDictionaryKeyExtraction(dictionaryKeyExtraction).
				withReplaceExpression(new ArrayAccess(0));

			// final ReplaceBase<?> replacedElements = this.defaultExpression == FILTER_RECORDS ?
			// new ElementStrictReplace().withInputs(arraySplit, right) :
			// new ElementReplaceWithDefault().withDefaultExpression(this.defaultExpression).withInputs(arraySplit,
			// right);

			final AssembleArray arrayDictionary = new AssembleArray().
				withInputs(replacedElements);

			final Replace arrayLookup = new Replace().
				withInputs(sopremoModule.getInput(0), arrayDictionary).
				withReplaceExpression(this.replaceExpression);
			// empty arrays will not be replaced
			Selection emptyArrays = new Selection().
				withCondition(new UnaryExpression(this.replaceExpression, true)).
				withInputs(sopremoModule.getInput(0));
			sopremoModule.getOutput(0).setInput(0, new UnionAll().withInputs(arrayLookup, emptyArrays));
		} else {
			ReplaceBase<?> replaceAtom;
			if (defaultExpression == FILTER_RECORDS)
				replaceAtom = new StrictReplace();
			else
				replaceAtom = new ReplaceWithDefaultValue().withDefaultExpression(this.defaultExpression);

			replaceAtom.withInputs(sopremoModule.getInputs()).
				withReplaceExpression(this.replaceExpression).
				withDictionaryValueExtraction(this.dictionaryValueExtraction).
				withKeyExpressions(0, getReplaceExpression()).
				withKeyExpressions(1, getDictionaryKeyExtraction());
			sopremoModule.getOutput(0).setInput(0,
				replaceAtom.withInputs(sopremoModule.getInput(0), sopremoModule.getInput(1)));
		}

		return sopremoModule.asElementary();
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

	public static class AssembleArray extends ElementaryOperator<AssembleArray> {
		/**
	 *
	 */
		private static final long serialVersionUID = 7334161941683036846L;

		public static class Implementation extends SopremoReduce {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(IArrayNode values, JsonCollector out) {
				IJsonNode[] array = new IJsonNode[((IArrayNode) key).size()];
				int replacedCount = 0;
				for (IJsonNode value : values) {
					int index = ((NumericNode) ((IArrayNode) value).get(0)).getIntValue();
					IJsonNode element = ((IArrayNode) value).get(1);
					array[index] = element;
					replacedCount++;
				}

				// all values replaced
				if (replacedCount == array.length)
					out.collect(key, JsonUtil.asArray(array));
			}
		}
	}

}