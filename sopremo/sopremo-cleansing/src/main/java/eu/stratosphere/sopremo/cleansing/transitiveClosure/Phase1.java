package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class Phase1 extends ElementaryOperator<Phase1> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6221666263918022600L;

	public static class Implementation extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
		 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		public void map(JsonNode key, JsonNode matrix,
				JsonCollector out) {
			if (((ArrayNode) key).get(0).equals(((ArrayNode) key).get(1))) {
				TransitiveClosure.warshall((BinarySparseMatrix) matrix/*, (BinarySparseMatrix) matrix*/);
				out.collect(key, matrix);
			}

		}
	}

}
