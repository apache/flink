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
package eu.stratosphere.sopremo.sdaa11.clustering.treecreation;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;

/**
 * @author skruse
 *
 */
public class TreeCreator extends CompositeOperator<TreeCreator> {

	private static final long serialVersionUID = 1450138351751038162L;
	
	/**
	 * Specifies the degree of the created tree.
	 */
	private int treeWidth = TreeAssembler.DEFAULT_TREE_WIDTH;

	/**
	 * Returns the treeWidth.
	 * 
	 * @return the treeWidth
	 */
	public int getTreeWidth() {
		return treeWidth;
	}

	/**
	 * Sets the treeWidth to the specified value.
	 *
	 * @param treeWidth the treeWidth to set
	 */
	public void setTreeWidth(int treeWidth) {
	
		this.treeWidth = treeWidth;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

		final Operator<?> input = module.getInput(0);
		final ClusterToTreePreparation preparation = new ClusterToTreePreparation().withInputs(input);
		final TreeAssembler assembler = new TreeAssembler()
				.withInputs(preparation);
		assembler.setTreeWidth(treeWidth);

		module.getOutput(0).setInput(0, assembler);

		return module;
	}

}
