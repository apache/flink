package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;

/**
 * Represents the first phase of the algorithm used in {@link TransitiveClosure}. The whole matrix serves as input but
 * only diagonal blocks with a specific id will be processed by the
 * {@link TransitiveClosure#warshall(BinarySparseMatrix)} algorithm. The result of this operator is the whole, modified
 * matrix.
 */
public class Phase1 extends ElementaryOperator<Phase1> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6221666263918022600L;

	private int iterationStep;

	/**
	 * Sets the iteration step. This step is used to specify the block that should be processed (if step = 2 only block
	 * (2,2) will be processed).
	 * 
	 * @param iterationStep
	 *        the iteration step
	 */
	public void setIterationStep(Integer iterationStep) {
		if (iterationStep == null)
			throw new NullPointerException("iterationStep must not be null");

		this.iterationStep = iterationStep;
	}

	public static class Implementation extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

		private int iterationStep;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
		 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		public void map(JsonNode key, JsonNode matrix,
				JsonCollector out) {

			IntNode itStep = new IntNode(this.iterationStep);

			if (((ArrayNode) key).get(0).equals(itStep) && ((ArrayNode) key).get(1).equals(itStep)) {
				TransitiveClosure.warshall((BinarySparseMatrix) matrix/* , (BinarySparseMatrix) matrix */);
			}
			out.collect(key, matrix);
		}
	}

}
