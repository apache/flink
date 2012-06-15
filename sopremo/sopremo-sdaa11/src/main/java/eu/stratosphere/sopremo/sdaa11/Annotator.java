package eu.stratosphere.sopremo.sdaa11;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class Annotator extends ElementaryOperator<Annotator> {

	private static final long serialVersionUID = 1243242341L;

	public static final IntNode ANNOTATION_VALUE = new IntNode(0);

	public static class Implementation extends SopremoMap {
		private final ObjectNode output = new ObjectNode();

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			System.out.println("Annotating " + value);
			AnnotatorNodes.annotate(this.output, ANNOTATION_VALUE, value);
			System.out.println(this.output);
			out.collect(this.output);
		}

	}

}
