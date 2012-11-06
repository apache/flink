package eu.stratosphere.sopremo.base;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.rewrite.ReplaceInputSelectionWithArray;
import eu.stratosphere.util.IsInstancePredicate;

@InputCardinality(min = 2)
@OutputCardinality(1)
@Name(verb = "join")
public class Join extends CompositeOperator<Join> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6428723643579047169L;

	private BooleanExpression joinCondition = new AndExpression();

	private transient List<BinaryBooleanExpression> binaryConditions = new ArrayList<BinaryBooleanExpression>();

	private EvaluationExpression resultProjection = ObjectCreation.CONCATENATION;

	private final IntSet outerJoinSources = new IntOpenHashSet();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		switch (this.binaryConditions.size()) {
		case 0:
			throw new IllegalStateException("No join condition specified");
		case 1:
			// only two way join
			final TwoSourceJoin join = new TwoSourceJoin().
				withOuterJoinIndices(this.outerJoinSources.toIntArray()).
				withInputs(module.getInputs()).
				withCondition(this.binaryConditions.get(0)).
				withResultProjection(this.getResultProjection());
			module.getOutput(0).setInput(0, join);
			break;

		default:
			List<TwoSourceJoin> joins = this.getInitialJoinOrder(module);
			// return new TwoSourceJoin().
			// withCondition(this.joinCondition).
			// withInputs(getInputs()).
			// asElementaryOperators();

			final List<JsonStream> inputs = new ArrayList<JsonStream>();
			int numInputs = this.getNumInputs();
			for (int index = 0; index < numInputs; index++)
				inputs.add(OperatorUtil.positionEncode(module.getInput(index), index, numInputs));

			for (final TwoSourceJoin twoSourceJoin : joins) {
				List<JsonStream> operatorInputs = twoSourceJoin.getInputs();

				final List<JsonStream> actualInputs = new ArrayList<JsonStream>(operatorInputs.size());
				List<Source> moduleInput = module.getInputs();
				for (int index = 0; index < operatorInputs.size(); index++) {
					final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getSource().getOperator());
					actualInputs.add(inputs.get(inputIndex).getSource());
				}
				for (int index = 0; index < operatorInputs.size(); index++) {
					final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getSource().getOperator());
					inputs.set(inputIndex, twoSourceJoin);
				}
				twoSourceJoin.setInputs(actualInputs);
				twoSourceJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));
			}

			final TwoSourceJoin lastJoin = joins.get(joins.size() - 1);
			EvaluationExpression resultProjection = this.getResultProjection();
			resultProjection.replace(new IsInstancePredicate(InputSelection.class),
				new ReplaceInputSelectionWithArray());
			module.getOutput(0).setInput(0,
				new Projection().withInputs(lastJoin).withResultProjection(resultProjection));
		}
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.joinCondition.equals(((Join) obj).joinCondition)
			&& this.outerJoinSources.equals(((Join) obj).outerJoinSources)
			&& this.resultProjection.equals(((Join) obj).resultProjection);
	}

	public BooleanExpression getJoinCondition() {
		return this.joinCondition;
	}

	public int[] getOuterJoinIndices() {
		return this.outerJoinSources.toIntArray();
	}

	public EvaluationExpression getOuterJoinSources() {
		EvaluationExpression[] expressions = new EvaluationExpression[this.outerJoinSources.size()];
		final IntIterator iterator = this.outerJoinSources.iterator();
		for (int index = 0; iterator.hasNext(); index++) {
			final int inputIndex = iterator.nextInt();
			expressions[index] = new InputSelection(inputIndex);
		}
		return new ArrayCreation(expressions);
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.joinCondition.hashCode();
		result = prime * result + this.outerJoinSources.hashCode();
		result = prime * result + this.resultProjection.hashCode();
		return result;
	}

	@Property
	@Name(preposition = "where")
	public void setJoinCondition(BooleanExpression joinCondition) {
		if (joinCondition == null)
			throw new NullPointerException("joinCondition must not be null");

		final ArrayList<BinaryBooleanExpression> expressions = new ArrayList<BinaryBooleanExpression>();
		this.addBinaryExpressions(joinCondition, expressions);
		if (expressions.size() == 0)
			throw new IllegalArgumentException("No join condition given");

		this.joinCondition = joinCondition;
		this.binaryConditions = expressions;
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		final ArrayList<BinaryBooleanExpression> expressions = new ArrayList<BinaryBooleanExpression>();
		this.addBinaryExpressions(this.joinCondition, expressions);
		this.binaryConditions = expressions;
	}

	public void setOuterJoinIndices(int... outerJoinIndices) {
		if (outerJoinIndices == null)
			throw new NullPointerException("outerJoinIndices must not be null");

		this.outerJoinSources.clear();
		for (int index : outerJoinIndices)
			this.outerJoinSources.add(index);
	}

	@Property
	@Name(verb = "preserve")
	public void setOuterJoinSources(EvaluationExpression outerJoinSources) {
		if (outerJoinSources == null)
			throw new NullPointerException("outerJoinSources must not be null");
		final Iterable<? extends EvaluationExpression> expressions;
		if (outerJoinSources instanceof InputSelection)
			expressions = Collections.singleton(outerJoinSources);
		else if (outerJoinSources instanceof ArrayCreation)
			expressions = (ArrayCreation) outerJoinSources;
		else
			throw new IllegalArgumentException(String.format("Cannot interpret %s", outerJoinSources));

		this.outerJoinSources.clear();
		for (EvaluationExpression expression : expressions)
			this.outerJoinSources.add(((InputSelection) expression).getIndex());
	}

	@Property
	@Name(preposition = "into")
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.getJoinCondition());
		if (this.getResultProjection() != EvaluationExpression.VALUE)
			builder.append(" to ").append(this.getResultProjection());
		return builder.toString();
	}

	public Join withJoinCondition(BooleanExpression joinCondition) {
		this.setJoinCondition(joinCondition);
		return this;
	}

	public Join withOuterJoinIndices(int... outerJoinIndices) {
		this.setOuterJoinIndices(outerJoinIndices);
		return this;
	}

	public Join withOuterJoinSources(EvaluationExpression outerJoinSources) {
		this.setOuterJoinSources(outerJoinSources);
		return this;
	}

	public Join withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	private void addBinaryExpressions(BooleanExpression joinCondition, List<BinaryBooleanExpression> expressions) {
		if (joinCondition instanceof BinaryBooleanExpression)
			expressions.add((BinaryBooleanExpression) joinCondition);
		else if (joinCondition instanceof AndExpression)
			for (BooleanExpression expression : ((AndExpression) joinCondition).getExpressions())
				this.addBinaryExpressions(expression, expressions);
		else
			throw new IllegalArgumentException("Cannot handle expression " + joinCondition);
	}

	private List<TwoSourceJoin> getInitialJoinOrder(SopremoModule module) {
		final List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (final BinaryBooleanExpression expression : this.binaryConditions)
			joins.add(this.getTwoSourceJoinForExpression(expression, module));

		// TODO: add some kind of optimization?
		return joins;
	}

	private TwoSourceJoin getTwoSourceJoinForExpression(final BinaryBooleanExpression binaryCondition,
			SopremoModule module) {
		List<EvaluationExpression> inputSelections =
			binaryCondition.findAll(new IsInstancePredicate(InputSelection.class));
		if (inputSelections.size() != 2)
			throw new IllegalArgumentException(String.format("Condition must refer to two source: %s", binaryCondition));

		BinaryBooleanExpression adjustedExpression = (BinaryBooleanExpression) binaryCondition.clone();
		final int firstIndex = ((InputSelection) inputSelections.get(0)).getIndex();
		final int secondIndex = ((InputSelection) inputSelections.get(1)).getIndex();
		adjustedExpression.replace(inputSelections.get(0), new PathExpression(new InputSelection(0), new ArrayAccess(
			firstIndex)));
		adjustedExpression.replace(inputSelections.get(1), new PathExpression(new InputSelection(1), new ArrayAccess(
			secondIndex)));

		IntList outerJoinIndices = new IntArrayList();
		if (this.outerJoinSources.contains(firstIndex))
			outerJoinIndices.add(0);
		if (this.outerJoinSources.contains(secondIndex))
			outerJoinIndices.add(1);
		return new TwoSourceJoin().withOuterJoinIndices(outerJoinIndices.toIntArray()).
			withInputs(module.getInput(firstIndex), module.getInput(secondIndex)).
			withCondition(adjustedExpression);
	}

}
