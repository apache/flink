package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

@Name(verb = "union all")
public class UnionAll extends CompositeOperator<UnionAll> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	@Override
	public SopremoModule asElementaryOperators() {

		final List<JsonStream> inputs = this.getInputs();
		final SopremoModule module = new SopremoModule(this.getName(), inputs.size(), 1);

		Operator<?> leftInput = module.getInput(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputUnionAll().withInputs(leftInput, module.getInput(index));

		module.getOutput(0).setInput(0, leftInput);

		return module;
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputUnionAll extends ElementaryOperator<TwoInputUnionAll> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6943661062249973782L;

		// TODO: replace with efficient union operator
		public static class TwoInputUnion extends
				SopremoCoGroup<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void coGroup(final JsonNode key, final ArrayNode values1, final ArrayNode values2,
					final JsonCollector out) {
				for (final JsonNode value : values1)
					out.collect(key, value);
				for (final JsonNode value : values2)
					out.collect(key, value);
			}
		}
	}
}
