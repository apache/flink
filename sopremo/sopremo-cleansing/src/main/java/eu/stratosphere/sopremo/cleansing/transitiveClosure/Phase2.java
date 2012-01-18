package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class Phase2 extends CompositeOperator<Phase2> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7553776835899633516L;

	private int iterationStep;
	
	public void setIterationStep(Integer iterationStep) {
		if (iterationStep == null)
			throw new NullPointerException("iterationStep must not be null");

		this.iterationStep = iterationStep;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream phase1 = sopremoModule.getInput(0);

		final ExtractDiagonal transDia = new ExtractDiagonal().withInputs(phase1);
		transDia.setIterationStep(this.iterationStep);
		
		final ExtractNonRelatingBlocks nonRelatingBlocks = new ExtractNonRelatingBlocks().withInputs(phase1);
		nonRelatingBlocks.setIterationStep(this.iterationStep);
		
		final GenerateRows rows = new GenerateRows().withInputs(phase1);
		rows.setIterationStep(this.iterationStep);
		final ComputeBlockTuples computeRows = new ComputeBlockTuples().withInputs(transDia, rows);

		final GenerateColumns columns = new GenerateColumns().withInputs(phase1);
		columns.setIterationStep(this.iterationStep);
		final ComputeBlockTuples computeColumns = new ComputeBlockTuples().withInputs(transDia, columns);
		
		UnionAll union = new UnionAll().withInputs(transDia, computeRows, computeColumns, nonRelatingBlocks);
		
		GenerateFullMatrix fullMatrix = new GenerateFullMatrix().withInputs(union);
		
		sopremoModule.getOutput(0).setInput(0, (this.iterationStep == 0)? fullMatrix : union);

		return sopremoModule;
	}

	private static class ExtractDiagonal extends ElementaryOperator<ExtractDiagonal> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -482320275922871444L;
		
		private int iterationStep;
		
		public void setIterationStep(Integer iterationStep) {
			if (iterationStep == null)
				throw new NullPointerException("iterationStep must not be null");

			this.iterationStep = iterationStep;
		}

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
				IntNode itStep = new IntNode(this.iterationStep);
				
				if(((ArrayNode) key).get(0).equals(itStep) && ((ArrayNode) key).get(1).equals(itStep)){
					out.collect(key, value);
				}
			}

		}
	}
	
	private static class ExtractNonRelatingBlocks extends ElementaryOperator<ExtractNonRelatingBlocks> {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -4505211267174986216L;
		
		private int iterationStep;
		
		public void setIterationStep(Integer iterationStep) {
			if (iterationStep == null)
				throw new NullPointerException("iterationStep must not be null");

			this.iterationStep = iterationStep;
		}

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
				ArrayNode castedKey = (ArrayNode) key;
				IntNode itStep = new IntNode(this.iterationStep);

				if (!castedKey.get(0).equals(itStep) && !castedKey.get(1).equals(itStep)) {
					out.collect(key, value);
				}
			}
		}
	}

	private static class GenerateRows extends ElementaryOperator<GenerateRows> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3480847243868518647L;
		
		private int iterationStep;
		
		public void setIterationStep(Integer iterationStep) {
			if (iterationStep == null)
				throw new NullPointerException("iterationStep must not be null");

			this.iterationStep = iterationStep;
		}

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
				ArrayNode castedKey = (ArrayNode) key;
				IntNode itStep = new IntNode(this.iterationStep);

				if (!castedKey.get(0).equals(castedKey.get(1)) && castedKey.get(0).equals(itStep)) {
					out.collect(key, value);
				}
			}
		}
	}

	private static class GenerateColumns extends ElementaryOperator<GenerateColumns> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2720743851827014023L;

		private int iterationStep;
		
		public void setIterationStep(Integer iterationStep) {
			if (iterationStep == null)
				throw new NullPointerException("iterationStep must not be null");

			this.iterationStep = iterationStep;
		}
		
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
				ArrayNode castedKey = (ArrayNode) key;
				IntNode itStep = new IntNode(this.iterationStep);

				if (!castedKey.get(0).equals(castedKey.get(1)) && castedKey.get(1).equals(itStep)) {
					out.collect(key, value);
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
		public static class Implementation extends SopremoCross<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMatch#match(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out) {

				JsonNode itStep = ((ArrayNode) key1).get(0);
				
				BinarySparseMatrix current = (BinarySparseMatrix) value2;
				
				//if the two blocks are in the same column, we have to use another 3BSP warshall
				if(itStep.equals(((ArrayNode)key2).get(1))){
					TransitiveClosure.warshall(current, (BinarySparseMatrix) value1, current);

				} else {
					TransitiveClosure.warshall((BinarySparseMatrix) value1, current, current);

				}
				
				out.collect(key2, current);
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
