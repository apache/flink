package eu.stratosphere.sopremo.sdaa11;

import java.util.Random;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class RoundRobinAnnotator extends
		ElementaryOperator<RoundRobinAnnotator> {

	private static final long serialVersionUID = 1243242341L;

	public static final int DEFAULT_MAX_ANNOTATION = 0;

	private int maxAnnotation = DEFAULT_MAX_ANNOTATION;

	/**
	 * Returns the maxAnnotation.
	 * 
	 * @return the maxAnnotation
	 */
	public int getMaxAnnotation() {
		return this.maxAnnotation;
	}

	/**
	 * Sets the maxAnnotation to the specified value.
	 * 
	 * @param maxAnnotation
	 *            the maxAnnotation to set
	 */
	public void setMaxAnnotation(final int maxAnnotation) {
		this.maxAnnotation = maxAnnotation;
	}

	public static class Implementation extends SopremoMap {

		int maxAnnotation;

		private final ObjectNode output = new ObjectNode();
		private final IntNode annotationNode = new IntNode();
		private int currentAnnotation = new Random()
				.nextInt(this.maxAnnotation + 1);

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			this.currentAnnotation = this.currentAnnotation >= this.maxAnnotation ? 0
					: this.currentAnnotation + 1;
			this.annotationNode.setValue(this.currentAnnotation);
			// System.out.println("Annotating " + value + " with "
			// + this.annotationNode);
			AnnotatorNodes.annotate(this.output, this.annotationNode, value);
			// System.out.println(this.output);
			out.collect(this.output);
		}

	}

}
