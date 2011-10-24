package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.ArraySplit;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.cleansing.record_linkage.ClosureMode;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream input = sopremoModule.getInput(0);

		// Partitioning
		Partitioning partitioning = new Partitioning().withInputs(input);
		Grouping group = new Grouping().withInputs(partitioning).withGroupingKey(EvaluationExpression.KEY)
			.withResetKey(false);

		// Generate Matrix
		final GenerateMatrix genMatrix = new GenerateMatrix().withInputs(group);

		// compute transitive Closure
		final Phase1 phase1 = new Phase1().withInputs(genMatrix);

		// emit Results as Links
		final EmitMatrix result = new EmitMatrix().withInputs(phase1);

		sopremoModule.getOutput(0).setInput(0, result);

		return sopremoModule;

	}

	private static class Partitioning extends ElementaryOperator<Partitioning> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5940876439025744020L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				ArrayNode valueArray = new ArrayNode(((ArrayNode) value).get(0), ((ArrayNode) value).get(1));
				ArrayNode keyArray = new ArrayNode(((ObjectNode) valueArray.get(0)).get("partition"),
					((ObjectNode) valueArray.get(1)).get("partition"));
				out.collect(keyArray, valueArray);
			}
		}
	}

	public static void warshall(final BinarySparseMatrix matrix) {
		// Warshall
		for (final JsonNode row : matrix.getRows()) {
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				for (final JsonNode transitiveNode : matrix.get(column))
					if (!row.equals(transitiveNode) && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
	}

	private static class GenerateMatrix extends ElementaryOperator<GenerateMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5940876439025744020L;

		@SuppressWarnings("unused")
		public static class Implementation extends MapStub<JsonNode, JsonNode, JsonNode, BinarySparseMatrix> {

			@Override
			public void map(JsonNode key, JsonNode pairs, Collector<JsonNode, BinarySparseMatrix> out) {
				BinarySparseMatrix matrix = new BinarySparseMatrix();

				for (final JsonNode pair : (ArrayNode) SopremoUtil.unwrap(pairs)) {
					JsonNode value1 = null, value2 = null;
					for (int sourceIndex = 0; sourceIndex < ((ArrayNode) pair).size(); sourceIndex++) {
						JsonNode value = ((ArrayNode) pair).get(sourceIndex);
						if (value != NullNode.getInstance())
							if (value1 == null)
								value1 = value;
							else {
								value2 = value;
								break;
							}
					}
					matrix.set(value1, value2);
					matrix.set(value2, value1);
				}
				out.collect(key, matrix);
			}

		}
	}

	private static class EmitMatrix extends ElementaryOperator<EmitMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2384047858154432955L;

		@SuppressWarnings("unused")
		public static class Implementation extends MapStub<JsonNode, BinarySparseMatrix, JsonNode, JsonNode> {

			@Override
			public void map(JsonNode key, BinarySparseMatrix genMatrix, Collector<JsonNode, JsonNode> out) {
				BinarySparseMatrix matrix = (BinarySparseMatrix) genMatrix;
				for (final JsonNode row : matrix.getRows())
					for (final JsonNode column : matrix.get(row))
						if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
							out.collect(/* key */NullNode.getInstance(), JsonUtil.asArray(row, column));
			}
		}
	}

	/**
	 * @param closureMode
	 * @return
	 */
	public TransitiveClosure withClosureMode(ClosureMode closureMode) {
		return null;
	}
}

// /**
// * Fills a matrix with pairs of tuples
// *
// * @param genMatrix
// * input matrix
// * @param pairs
// * pairs to be filled in the matrix
// */
// protected void fillMatrix(BinarySparseMatrix<?> genMatrix, final JsonNode pairs) {
// @SuppressWarnings("unchecked")
// BinarySparseMatrix<JsonNode> matrix = (BinarySparseMatrix<JsonNode>) genMatrix;
//
// for (final JsonNode pair : (ArrayNode) pairs) {
// JsonNode value1 = null, value2 = null;
// for (int sourceIndex = 0; sourceIndex < ((ArrayNode) pair).size(); sourceIndex++) {
// JsonNode value = ((ArrayNode) pair).get(sourceIndex);
// if (value != NullNode.getInstance())
// if (value1 == null)
// value1 = value;
// else {
// value2 = value;
// break;
// }
// }
// matrix.set(value1, value2);
// matrix.set(value2, value1);
// }
// }
