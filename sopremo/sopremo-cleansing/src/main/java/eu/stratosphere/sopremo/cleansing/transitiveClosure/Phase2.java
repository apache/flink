package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class Phase2 extends ElementaryOperator<Phase2> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3402040826166875828L;

	public static class Implementation extends ReduceStub<JsonNode, BinarySparseMatrix, JsonNode, BinarySparseMatrix> {

		@Override
		public void reduce(JsonNode key, Iterator<BinarySparseMatrix> values,
				Collector<JsonNode, BinarySparseMatrix> out) {
			BinarySparseMatrix matrix = this.mergeMatrix(values);

			TransitiveClosure.warshall(matrix);
			
			out.collect(key, matrix);
		}

		private BinarySparseMatrix mergeMatrix(Iterator<BinarySparseMatrix> it) {
			BinarySparseMatrix matrix = it.next();

			while (it.hasNext()) {
				BinarySparseMatrix nextMatrix = it.next();

				for (JsonNode row : nextMatrix.getRows()) {
					matrix.setAll(row, nextMatrix.get(row));
				}
			}

			return matrix;
		}
	}
}
