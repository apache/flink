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
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoMap;

@InputCardinality(min = 2, max = 2)
public class GenerateMatrix extends CompositeOperator<GenerateMatrix> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9114512991148137780L;

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
		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream input = sopremoModule.getInput(0);
		JsonStream nullInput = sopremoModule.getInput(1);

		int n = this.numberOfPartitions;

		// Partitioning
		Partitioning partitioning = new Partitioning().withInputs(input);
		partitioning.setNumberOfPartitions(n);
		Grouping group = new Grouping().withInputs(partitioning).withGroupingKey(EvaluationExpression.KEY)
			.withResetKey(false);

		// Generate Matrix
		final GenerateBinarySparseMatrix genMatrix = new GenerateBinarySparseMatrix().withInputs(group);

		// generate empty blocks (see next step)
		final GenerateEmptyMatrix emptyMatrix = new GenerateEmptyMatrix().withInputs(nullInput);
		emptyMatrix.setN(n);

		// fill-up missing block in genMatrix
		final FillMatrix filledMatrix = new FillMatrix().withInputs(genMatrix, emptyMatrix);

		sopremoModule.getOutput(0).setInput(0, filledMatrix);

		return sopremoModule;
	}

	private static class Partitioning extends ElementaryOperator<Partitioning> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5940876439025744020L;

		private int numberOfPartitions;

		public void setNumberOfPartitions(int number) {
			this.numberOfPartitions = number;
		}

		@SuppressWarnings("unused")
		public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

			private int numberOfPartitions;

			@Override
			protected void map(JsonNode key, JsonNode value, JsonCollector out) {
				JsonNode value1 = ((ArrayNode) value).get(0);
				JsonNode value2 = ((ArrayNode) value).get(1);
				int id1 = ((IntNode) ((ObjectNode) value1).get("id")).getIntValue();
				int id2 = ((IntNode) ((ObjectNode) value2).get("id")).getIntValue();
				/*
				 * for any reason, phase 3 does not like partitions with index 0
				 * thats why, we let them run from 1 to n instead of 0 to n-1
				 */
				//IntNode partition1 = new IntNode(id1 != this.numberOfPartitions ? id1 % this.numberOfPartitions : id1);
				//IntNode partition2 = new IntNode(id2 != this.numberOfPartitions ? id2 % this.numberOfPartitions : id2);
				IntNode partition1 = new IntNode(id1 % this.numberOfPartitions);
				IntNode partition2 = new IntNode(id2 % this.numberOfPartitions);
				if (partition1.compareTo(partition2) <= 0) {
					out.collect(new ArrayNode(partition1, partition2), new ArrayNode(value1, value2));
				} else {
					out.collect(new ArrayNode(partition2, partition1), new ArrayNode(value2, value1));
				}
			}
		}
	}

	private static class GenerateBinarySparseMatrix extends ElementaryOperator<GenerateBinarySparseMatrix> {

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
				for (int i = 0; i < n; i++) {
					for (int j = 0; j <= i; j++) {
						out.collect(new ArrayNode(new IntNode(j), new IntNode(i)), new BinarySparseMatrix());
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
				if (values1.isEmpty()) {
					out.collect(key, values2.get(0));
				} else {
					out.collect(key, values1.get(0));
				}
			}
		}
	}

}
