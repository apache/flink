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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author skruse
 * 
 */
@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class CreateFrequentItemsetCandidates extends
		ElementaryOperator<CreateFrequentItemsetCandidates> {

	private static final long serialVersionUID = -3392494979410936691L;

	private int fisSize = 1;

	/**
	 * Returns the targeted frequent item set size.
	 * 
	 * @return the targeted frequent item set size 
	 */
	public int getFisSize() {
		return fisSize;
	}

	/**
	 * Sets the targeted frequent item set size to the specified value.
	 *
	 * @param fisSize the targeted frequent item set size to set
	 */
	public void setFisSize(int fisSize) {
		this.fisSize = fisSize;
	}
	
	public static class Implementation extends SopremoMap {

		int fisSize;

		private ArrayNode itemsInputNode = new ArrayNode();
		private ObjectNode outputNode = new ObjectNode();
		private ArrayNode itemsOutputNode = new ArrayNode();
		private int[] iterator;
		
		public Implementation() {
			FrequentItemsetNodes.write(outputNode, itemsOutputNode);
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			// Make sure that the values are materialized!
			itemsInputNode.clear();
			itemsInputNode.addAll(FrequentItemsetNodes.getItems((ObjectNode) value));

			resetIterator();
			do {
				itemsOutputNode.clear();
				for (int i : iterator) {
					itemsOutputNode.add(itemsInputNode.get(i));
				}
				out.collect(outputNode);
			} while (moveIterator(itemsInputNode.size()));
		}

		/**
		 * @return
		 */
		private boolean moveIterator(int inputSize) {
			for (int i = 0 ; i < iterator.length - 1 ; i++) {
				// Check whether next index cannot be moved forward any more.
				if (iterator[i+1] == (inputSize - iterator.length) + i+1) {
					// If so, we can move the current operator forward and
					// reset the follower.
					iterator[i] = iterator[i+1] = iterator[i] + 1;
				}
			}
			return ++iterator[iterator.length - 1] < inputSize;
		}

		/**
		 * 
		 */
		private void resetIterator() {
			if (iterator == null) {
				iterator = new int[fisSize];
			}
			for (int i = 0; i < iterator.length; i++) {
				iterator[i] = i;
			}
		}
		
	}
	
	
}
