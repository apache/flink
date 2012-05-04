package eu.stratosphere.sopremo.sdaa11;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class Annotator extends ElementaryOperator<Annotator> {

	private static final long serialVersionUID = 1243242341L;

	/**
	 * Key in output JSON object for the annotated object.
	 */
	public static final String OBJECT_KEY = "object";

	public static final String DUMMY_KEY = "dummy";

	public static final int DUMMY_VALUE_INDEX = 1;
	public static final IntNode DUMMY_VALUE = new IntNode(0);

	public static class Implementation extends SopremoMap {
		private final ObjectNode output = new ObjectNode();

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			System.out.println("Annotating "+value);
			this.output.put(OBJECT_KEY, value);
			this.output.put(DUMMY_KEY, DUMMY_VALUE);
			out.collect(this.output);
		}

	}

	public static IJsonNode deannotate(final IJsonNode node) {
		if (node == null || !(node instanceof ObjectNode))
			throw new IllegalArgumentException("Cannot deannotate " + node);

		return ((ObjectNode) node).get(OBJECT_KEY);
	}

}
