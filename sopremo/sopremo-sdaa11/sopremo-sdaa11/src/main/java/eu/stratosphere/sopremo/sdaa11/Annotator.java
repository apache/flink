package eu.stratosphere.sopremo.sdaa11;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class Annotator extends ElementaryOperator<Annotator> {

	private static final long serialVersionUID = 1243242341L;

	public static final int DUMMY_VALUE_INDEX = 1;
	public static final IntNode DUMMY_VALUE = new IntNode(0);

	public static class Implementation extends SopremoMap {

		private final ArrayNode output = new ArrayNode();

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			this.output.clear();
			this.output.add(value);
			this.output.add(DUMMY_VALUE);
			out.collect(this.output);
		}

	}

}
