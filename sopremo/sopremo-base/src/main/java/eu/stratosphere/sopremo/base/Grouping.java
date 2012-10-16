package eu.stratosphere.sopremo.base;

import java.util.IdentityHashMap;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.NullNode;

@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(verb = "group")
public class Grouping extends CompositeOperator<Grouping> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1452280003631381562L;

	private final static EvaluationExpression GROUP_ALL = new ConstantExpression(NullNode.getInstance());

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	private final Map<Operator<?>.Output, EvaluationExpression> keyExpressions =
		new IdentityHashMap<Operator<?>.Output, EvaluationExpression>();

	private EvaluationExpression defaultGroupingKey = GROUP_ALL;

	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		Operator<?> output;
		switch (this.getNumInputs()) {
		case 0:
			throw new IllegalStateException("No input given for grouping");
		case 1:
			output = new GroupProjection(this.resultProjection.remove(InputSelection.class)).
				withKeyExpression(0, this.getGroupingKey(0).remove(new InputSelection(0))).
				withInputs(module.getInputs());
			break;
		case 2:
			output = new CoGroupProjection(this.resultProjection).
				withKeyExpression(0, this.getGroupingKey(0).remove(new InputSelection(0))).
				withKeyExpression(1, this.getGroupingKey(1).remove(new InputSelection(1))).
				withInputs(module.getInputs());
			break;
		default:
			throw new IllegalStateException("More than two sources are not supported");
			// List<JsonStream> inputs = new ArrayList<JsonStream>();
			// List<EvaluationExpression> keyExpressions = new ArrayList<EvaluationExpression>();
			// for (int index = 0; index < numInputs; index++) {
			// inputs.add(OperatorUtil.positionEncode(module.getInput(index), index, numInputs));
			// keyExpressions.add(new PathExpression(new InputSelection(index), getGroupingKey(index)));
			// }
			// final UnionAll union = new UnionAll().
			// withInputs(inputs);
			// final PathExpression projection =
			// new PathExpression(new AggregationExpression(new ArrayUnion()), this.resultProjection);
			// output = new GroupProjection(projection).
			// withInputs(union);
			// break;
		}

		module.getOutput(0).setInput(0, output);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		final Grouping other = (Grouping) obj;
		return this.resultProjection.equals(other.resultProjection);
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.resultProjection.hashCode();
		return result;
	}

	@Property(preferred = true)
	@Name(preposition = "into")
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	public Grouping withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	@Property(preferred = true, input = true)
	@Name(preposition = "by")
	public void setGroupingKey(final int inputIndex, final EvaluationExpression keyExpression) {
		this.setGroupingKey(this.getSafeInput(inputIndex), keyExpression);
	}

	public void setGroupingKey(final JsonStream input, final EvaluationExpression keyExpression) {
		if (keyExpression == null)
			throw new NullPointerException("keyExpression must not be null");

		this.keyExpressions.put(input.getSource(), keyExpression);
	}

	public Grouping withGroupingKey(int inputIndex, EvaluationExpression groupingKey) {
		this.setGroupingKey(inputIndex, groupingKey);
		return this;
	}

	public Grouping withGroupingKey(EvaluationExpression groupingKey) {
		this.setDefaultGroupingKey(groupingKey);
		return this;
	}

	public EvaluationExpression getGroupingKey(final int index) {
		return this.getGroupingKey(this.getInput(index));
	}

	public EvaluationExpression getGroupingKey(final JsonStream input) {
		final Operator<?>.Output source = input == null ? null : input.getSource();
		EvaluationExpression keyExpression = this.keyExpressions.get(source);
		if (keyExpression == null)
			keyExpression = this.getDefaultGroupingKey();
		return keyExpression;
	}

	public EvaluationExpression getDefaultGroupingKey() {
		return this.defaultGroupingKey;
	}

	public void setDefaultGroupingKey(EvaluationExpression defaultGroupingKey) {
		if (defaultGroupingKey == null)
			throw new NullPointerException("defaultGroupingKey must not be null");

		this.defaultGroupingKey = defaultGroupingKey;
	}

	@Override
	public String toString() {
		return String.format("%s to %s", super.toString(), this.resultProjection);
	}

	@InputCardinality(min = 2, max = 2)
	public static class CoGroupProjection extends ElementaryOperator<CoGroupProjection> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 561729616462154707L;

		private EvaluationExpression projection = EvaluationExpression.VALUE;

		public CoGroupProjection(EvaluationExpression projection) {
			this.projection = projection;
		}

		public EvaluationExpression getProjection() {
			return this.projection;
		}

		public void setProjection(EvaluationExpression projection) {
			if (projection == null)
				throw new NullPointerException("projection must not be null");

			this.projection = projection;
		}

		public static class Implementation extends SopremoCoGroup {
			private CachingExpression<IJsonNode> projection;

			@Override
			protected void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out) {
				out.collect(this.projection.evaluate(JsonUtil.asArray(values1, values2), this.getContext()));
			}
		}
	}

	@InputCardinality(1)
	public static class GroupProjection extends ElementaryOperator<GroupProjection> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 561729616462154707L;

		@SuppressWarnings("unused")
		private final EvaluationExpression projection;

		public GroupProjection(final EvaluationExpression projection) {
			this.projection = projection;
		}

		public static class Implementation extends SopremoReduce {
			private CachingExpression<IJsonNode> projection;

			@Override
			protected void reduce(final IStreamArrayNode values, final JsonCollector out) {
				out.collect(this.projection.evaluate(values, this.getContext()));
			}
		}
	}
}
