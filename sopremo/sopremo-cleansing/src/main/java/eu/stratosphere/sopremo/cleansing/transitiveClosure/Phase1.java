package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class Phase1 extends ElementaryOperator<Phase1> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6221666263918022600L;

	public static class Implementation extends
			MapStub<JsonNode, BinarySparseMatrix, JsonNode, BinarySparseMatrix> {

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
		 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		public void map(JsonNode key, BinarySparseMatrix matrix,
				Collector<JsonNode, BinarySparseMatrix> out) {
			TransitiveClosure.warshall(matrix);

			out.collect(key, matrix);
		}
	}

}
