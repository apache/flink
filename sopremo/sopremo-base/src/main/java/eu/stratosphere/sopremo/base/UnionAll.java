package eu.stratosphere.sopremo.base;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

@Name(verb = "union all")
public class UnionAll extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	@Override
	public SopremoModule asElementaryOperators() {

		final List<Output> inputs = getInputs();
		final SopremoModule module = new SopremoModule(getName(), inputs.size(), 1);

		Operator leftInput = module.getInput(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputUnionAll().withInputs(leftInput, module.getInput(index));

		module.getOutput(0).setInput(0, leftInput);

		return module;
	}

	@InputCardinality(min = 2, max = 2)
	public static class TwoInputUnionAll extends ElementaryOperator {
		// TODO: replace with efficient union operator
		public static class TwoInputUnion extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				for (final JsonNode value : values1)
					out.collect(key, value);
				for (final JsonNode value : values2)
					out.collect(key, value);
			}
		}
	}
}
