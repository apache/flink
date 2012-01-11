package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
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

		//final ExtractMirroredMatrix mirroredMatrix = new ExtractMirroredMatrix().withInputs(computeRows);
		//final FillMatrix fillMatrix = new FillMatrix().withInputs(computeRows, mirroredMatrix);
		
		GenerateFullMatrix fullMatrix = new GenerateFullMatrix().withInputs(computeTuples);
		
		sopremoModule.getOutput(0).setInput(0, fullMatrix/*computeTuples/*fillMatrix*/);

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

				JsonNode oldKeyPrimary = key;
				JsonNode oldKeyCurrent = ((ArrayNode) value2).get(0);
				
				BinarySparseMatrix current = (BinarySparseMatrix) ((ArrayNode) value2).get(1);
				
				//if the two blocks are in the same column, we have to bring them into the same row -> transpose
				if(oldKeyPrimary.equals(((ArrayNode)oldKeyCurrent).get(1))){
					TransitiveClosure.warshall(current, (BinarySparseMatrix) value1, current);

					//current = current.transpose();
				} else {
					TransitiveClosure.warshall((BinarySparseMatrix) value1, current, current);

				}
				
				//transpose back
				//TransitiveClosure.warshall((BinarySparseMatrix) value1, current);
				
//				if(oldKeyPrimary.equals(((ArrayNode)oldKeyCurrent).get(1))){
//					current = current.transpose();
//				}
				
				out.collect(oldKeyCurrent, current);
			}

		}
	}
	
	private static class ExtractMirroredMatrix extends ElementaryOperator<ExtractMirroredMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 8542903311559435638L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				if(((ArrayNode) key).get(0).compareTo(((ArrayNode) key).get(1)) > 0){
					out.collect(new ArrayNode(((ArrayNode) key).get(1), ((ArrayNode) key).get(0)), value);
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
				if(values2.isEmpty()){
					out.collect(key, values1.get(0));
				} else {
					BinarySparseMatrix correctMatrix;
					BinarySparseMatrix wrongMatrix = (BinarySparseMatrix) values2.get(0);
					if(values1.isEmpty()){
						correctMatrix = new BinarySparseMatrix();
					} else {
						correctMatrix = (BinarySparseMatrix) values1.get(0);
					}
					for(JsonNode row : wrongMatrix.getRows()){
						for(JsonNode column : wrongMatrix.get(row)){
							correctMatrix.set(column, row);
						}
					}
					out.collect(key, correctMatrix);
				}
			}
		}
	}
	
	private static class GenerateFullMatrix extends ElementaryOperator<GenerateFullMatrix> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2835910815949750884L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			private int iterationStep;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				if (!((ArrayNode) key).get(0).equals(((ArrayNode) key).get(1))) {
					out.collect(new ArrayNode(((ArrayNode) key).get(1), ((ArrayNode) key).get(0)),
						((BinarySparseMatrix) value).transpose());
				}
				out.collect(key, value);

			}
		}
	}
}
