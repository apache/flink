package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;
public class Selection extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7687925343684319311L;

	private final BooleanExpression condition;

	public Selection(final BooleanExpression condition, final JsonStream input) {
		super(input);
		this.condition = condition;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((Selection) obj).condition);
	}

	public BooleanExpression getCondition() {
		return this.condition;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		return result;
	}

	//
	// @Override
	// public PactModule asPactModule(EvaluationContext context) {
	// PactModule module = new PactModule(this.toString(), 1, 1);
	// MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> selectionMap =
	// new MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
	// SelectionStub.class);
	// module.getOutput(0).setInput(selectionMap);
	// selectionMap.setInput(module.getInput(0));
	// SopremoUtil.serialize(selectionMap.getStubParameters(), "condition", this.getCondition());
	// SopremoUtil.setContext(selectionMap.getStubParameters(), context);
	// return module;
	// }

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		return builder.toString();
	}

	public static class SelectionStub extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private BooleanExpression condition;

		@Override
		protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
			if (this.condition.evaluate(value, this.getContext()) == BooleanNode.TRUE)
				out.collect(key, value);
		}

	}
}
