/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
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
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoMatch;

public class Phase3 extends CompositeOperator<Phase3> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5629802867631098519L;

	private int numberOfPartitions = 1;

	public void setNumberOfPartitions(int number) {
		this.numberOfPartitions = number;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {

		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 1, 1);
		JsonStream input = sopremoModule.getInput(0);

		int itCount = this.numberOfPartitions;

		ExtractNonRelatingBlocks diagonalBlock = new ExtractNonRelatingBlocks().withInputs(input);
		diagonalBlock.setIterationStep(itCount);

		TransformAKey a = new TransformAKey().withInputs(input);
		a.setIterationStep(itCount);
		TransformBKey b = new TransformBKey().withInputs(input);
		b.setIterationStep(itCount);
		TransformXKey x = new TransformXKey().withInputs(input);
		x.setIterationStep(itCount);
		BAndXMatch xb = new BAndXMatch().withInputs(b, x);
		AMatch axb = new AMatch().withInputs(a, xb);

		// final GenerateColumns columns = new GenerateColumns().withInputs(computeRows);
		// final ComputeBlockTuples computeTuples = new ComputeBlockTuples().withInputs(transDia, columns);

		sopremoModule.getOutput(0).setInput(0,
			new UnionAll().withInputs(diagonalBlock, a, new RestoreBKey().withInputs(b), axb));

		return sopremoModule;
	}

	private static class ExtractNonRelatingBlocks extends ElementaryOperator<ExtractNonRelatingBlocks> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -7207837316671752288L;

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
				if (((ArrayNode) key).get(0).equals(itStep) && ((ArrayNode) key).get(1).equals(itStep)) {
					out.collect(key, value);
				}
			}
		}
	}

	private static class TransformAKey extends ElementaryOperator<TransformAKey> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1296171168406180753L;

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
				if (((ArrayNode) key).get(1).equals(new IntNode(this.iterationStep))
					&& !((ArrayNode) key).get(0).equals(new IntNode(this.iterationStep)))
					out.collect(key, value);
			}
		}
	}

	private static class TransformBKey extends ElementaryOperator<TransformBKey> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8824655332341819085L;

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
				if (((ArrayNode) key).get(0).equals(new IntNode(this.iterationStep))
					&& !((ArrayNode) key).get(1).equals(new IntNode(this.iterationStep)))
					out.collect(((ArrayNode) key).get(1), new ArrayNode(key, value));
			}
		}
	}

	private static class TransformXKey extends ElementaryOperator<TransformXKey> {
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
				if (!((ArrayNode) key).get(0).equals(new IntNode(this.iterationStep))
					&& !((ArrayNode) key).get(1).equals(new IntNode(this.iterationStep))) {
					out.collect(((ArrayNode) key).get(1), new ArrayNode(key, value));
				}

			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	private static class BAndXMatch extends ElementaryOperator<BAndXMatch> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -1218248591344660494L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMatch<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.ArrayNode, eu.stratosphere.sopremo.jsondatamodel.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {

				ArrayNode oldKeyB = (ArrayNode) ((ArrayNode) value1).get(0);
				ArrayNode oldKeyX = (ArrayNode) ((ArrayNode) value2).get(0);
				out.collect(new ArrayNode(oldKeyX.get(0), oldKeyB.get(0)), new ArrayNode(value1, value2));

			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	private static class AMatch extends ElementaryOperator<AMatch> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2889622825342608482L;

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
				BinarySparseMatrix matrixA = (BinarySparseMatrix) value1;
				BinarySparseMatrix matrixB = (BinarySparseMatrix) ((ArrayNode) (((ArrayNode) value2).get(0))).get(1);
				BinarySparseMatrix matrixX = (BinarySparseMatrix) ((ArrayNode) (((ArrayNode) value2).get(1))).get(1);
				JsonNode oldKeyX = ((ArrayNode) (((ArrayNode) value2).get(1))).get(0);
				TransitiveClosure.warshall(matrixA, matrixB, matrixX);
				out.collect(oldKeyX, matrixX);
			}

		}
	}

	private static class RestoreBKey extends ElementaryOperator<RestoreBKey> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8850842639467506709L;

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.jsondatamodel.JsonNode,
			 * eu.stratosphere.sopremo.jsondatamodel.JsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				out.collect(((ArrayNode) value).get(0), ((ArrayNode) value).get(1));
			}
		}
	}
}
