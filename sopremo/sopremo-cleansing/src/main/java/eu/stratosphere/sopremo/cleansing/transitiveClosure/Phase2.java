package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.cleansing.record_linkage.BinarySparseMatrix;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoMatch;

public class Phase2 extends CompositeOperator<Phase2> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7553776835899633516L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream phase1 = sopremoModule.getInput(0);
		JsonStream matrix = sopremoModule.getInput(1);

		final TransformDiagonal transDia = new TransformDiagonal().withInputs(phase1);

		final GenerateRows rows = new GenerateRows().withInputs(matrix);
		final ComputeBlockTuples computeRows = new ComputeBlockTuples().withInputs(transDia, rows);

		final GenerateColumns columns = new GenerateColumns().withInputs(computeRows);
		final ComputeBlockTuples computeTuples = new ComputeBlockTuples().withInputs(transDia, columns);

		sopremoModule.getOutput(0).setInput(0, computeTuples);

		return sopremoModule;
	}

	private static class TransformDiagonal extends ElementaryOperator<TransformDiagonal> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -482320275922871444L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				out.collect(((ArrayNode) key).get(0), value);
			}

		}
	}

	private static class GenerateRows extends ElementaryOperator<GenerateRows> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3480847243868518647L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				ArrayNode castedKey = (ArrayNode) key;

				if (!castedKey.get(0).equals(castedKey.get(1))) {
					out.collect(castedKey.get(0), new ArrayNode(key, value));
				}
			}
		}
	}

	private static class GenerateColumns extends ElementaryOperator<GenerateColumns> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2720743851827014023L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				ArrayNode castedKey = (ArrayNode) key;

				if (!castedKey.get(0).equals(castedKey.get(1))) {
					out.collect(castedKey.get(1), new ArrayNode(key, value));
				}
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	private static class ComputeBlockTuples extends ElementaryOperator<ComputeBlockTuples> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -3317537635732054669L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMatch<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMatch#match(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {
				JsonNode oldKey = ((ArrayNode) value2).get(0);
				TransitiveClosure.warshall((BinarySparseMatrix) value1,
					(BinarySparseMatrix) ((ArrayNode) value2).get(1));

				out.collect(oldKey, ((ArrayNode) value2).get(1));
			}

		}
	}

}
