package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.record_linkage.ClosureMode;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoMap;

@InputCardinality(min = 2, max = 2)
public class TransitiveClosure extends CompositeOperator<TransitiveClosure> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7947908032635354614L;

	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream input = sopremoModule.getInput(0);
		JsonStream nullInput = sopremoModule.getInput(1);

		int n = 3;
		
		// Partitioning
		Partitioning partitioning = new Partitioning().withInputs(input);
		Grouping group = new Grouping().withInputs(partitioning).withGroupingKey(EvaluationExpression.KEY)
			.withResetKey(false);

		// Generate Matrix
		final GenerateMatrix genMatrix = new GenerateMatrix().withInputs(group);

		// generate empty blocks (see next step)
		final GenerateEmptyMatrix emptyMatrix = new GenerateEmptyMatrix().withInputs(nullInput);
		emptyMatrix.setN(n);
		
		// fill-up missing block in genMatrix
		final FillMatrix filledMatrix = new FillMatrix().withInputs(genMatrix, emptyMatrix);
		
		// compute transitive Closure P1
		final Phase1 phase1 = new Phase1().withInputs(filledMatrix);

		// compute transitive Closure P2
		final Phase2 phase2 = new Phase2().withInputs(phase1, filledMatrix);

		// compute transitive Closure P3
		final Phase3 phase3 = new Phase3().withInputs(new UnionAll().withInputs(phase1, phase2));

		// emit Results as Links
		final EmitMatrix result = new EmitMatrix().withInputs(/*new UnionAll().withInputs(phase1, phase2) */phase3);

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
				JsonNode value1 = ((ArrayNode) value).get(0);
				JsonNode value2 = ((ArrayNode) value).get(1);
				JsonNode partition1 = ((ObjectNode) value1).get("partition");
				JsonNode partition2 = ((ObjectNode) value2).get("partition");
				if (partition1.compareTo(partition2) <= 0){
					out.collect(new ArrayNode(partition1, partition2), new ArrayNode(value1, value2));
				} else {
					out.collect(new ArrayNode(partition2, partition1), new ArrayNode(value2, value1));
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
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			@Override
			public void map(JsonNode key, JsonNode pairs, JsonCollector out) {
				BinarySparseMatrix matrix = new BinarySparseMatrix();

				for (final JsonNode pair : (ArrayNode) pairs) {
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
					if (((ArrayNode) key).get(0).equals(((ArrayNode) key).get(1)))
						matrix.set(value2, value1);
				}
				out.collect(key, matrix);
			}

		}
	}

	private static class GenerateEmptyMatrix extends ElementaryOperator<GenerateEmptyMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -5424272405664960478L;

		private int n;

		public void setN(Integer n) {
			if (n == null)
				throw new NullPointerException("n must not be null");

			this.n = n;
		}

		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			private int n;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				for (int i=1; i<=n; i++){
					for(int j=1; j<=i; j++){
						out.collect(new ArrayNode(new IntNode(j),new IntNode(i)), new BinarySparseMatrix());
					}
				}

			}
		}
	}
	
	@InputCardinality(min = 2, max = 2)
	private static class FillMatrix extends ElementaryOperator<FillMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 7978217716893352313L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoCoGroup<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.ArrayNode, eu.stratosphere.sopremo.jsondatamodel.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(JsonNode key, ArrayNode values1, ArrayNode values2, JsonCollector out) {
				if(values1.isEmpty()){
					out.collect(key, values2.get(0));
				} else {
					out.collect(key, values1.get(0));
				}
			}
		}
	}

	private static class EmitMatrix extends ElementaryOperator<EmitMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2384047858154432955L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			@Override
			public void map(JsonNode key, JsonNode genMatrix, JsonCollector out) {
				BinarySparseMatrix matrix = (BinarySparseMatrix) genMatrix;
				for (final JsonNode row : matrix.getRows())
					for (final JsonNode column : matrix.get(row))
						if (JsonNodeComparator.INSTANCE.compare(row, column) < 0)
							out.collect(/* key */NullNode.getInstance(),
								JsonUtil.asArray(row, column));
			}
		}
	}

	public static void warshall(final BinarySparseMatrix matrix) {
		// Warshall
		for (JsonNode row : matrix.getRows()) {
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(matrix.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				Set<JsonNode> transitiveColumn = matrix.get(column);
				for (final JsonNode transitiveNode : transitiveColumn)
					if (!row.equals(transitiveNode) && !matrix.isSet(row, transitiveNode)) {
						matrix.set(row, transitiveNode);
						columnsToExplore.push(transitiveNode);
					}
			}
		}
		// matrix.makeSymmetric();
	}

	public static void warshall(final BinarySparseMatrix primary, BinarySparseMatrix current) {
		final Deque<JsonNode> rowsToExplore = new LinkedList<JsonNode>(current.getRows());
		while (!rowsToExplore.isEmpty()) {
			JsonNode row = rowsToExplore.pop();
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(primary.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				for (final JsonNode transitiveNode : current.get(row))
					if (!row.equals(transitiveNode) && !current.isSet(column, transitiveNode)) {
						current.set(column, transitiveNode);
						// columnsToExplore.push(transitiveNode);
					}
			}
		}
	}
	
	public static void warshall(final BinarySparseMatrix primaryRow, final BinarySparseMatrix primaryColumn, BinarySparseMatrix current) {
		final Deque<JsonNode> rowsToExplore = new LinkedList<JsonNode>(primaryRow.getRows());
		while (!rowsToExplore.isEmpty()){
			JsonNode row = rowsToExplore.pop();
			final Deque<JsonNode> columnsToExplore = new LinkedList<JsonNode>(primaryRow.get(row));
			while (!columnsToExplore.isEmpty()) {
				final JsonNode column = columnsToExplore.pop();
				final Deque<JsonNode> transitiveNodesToExplore = new LinkedList<JsonNode>(primaryColumn.get(column));
				while(!transitiveNodesToExplore.isEmpty()){
					final JsonNode transitiveNode = transitiveNodesToExplore.pop();
					current.set(row, transitiveNode);
				}


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
