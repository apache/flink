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
package eu.stratosphere.sopremo.sdaa11.clustering.main;

import temp.UnionAll;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.Source;

/**
 * @author skruse
 * 
 */
@InputCardinality(min = 3, max = 3)
public class ClusterRest extends CompositeOperator<ClusterRest> {

	private static final long serialVersionUID = -6900556381101996519L;

	public static final int SAMPLE_CLUSTERS_INPUT_INDEX = 0;
	public static final int REST_POINTS_INPUT_INDEX = 1;
	public static final int TREE_INPUT_INDEX = 2;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final ElementarySopremoModule module = new ElementarySopremoModule(this.getName(), 3, 1);

		final Source sampleClusterInput = module
				.getInput(SAMPLE_CLUSTERS_INPUT_INDEX);
		final Source restPointsInput = module.getInput(REST_POINTS_INPUT_INDEX);
		final Source treeInput = module.getInput(TREE_INPUT_INDEX);

		final ClusterDisassemble disassembleClusters = new ClusterDisassemble()
				.withInputs(sampleClusterInput.getOutput(0));

		final PointMapper pointMapper = new PointMapper();
		pointMapper.setInput(PointMapper.TREE_INPUT_INDEX, treeInput);
		pointMapper.setInput(PointMapper.POINT_INPUT_INDEX, restPointsInput);

		final UnionAll unionAll = new UnionAll().withInputs(
				disassembleClusters.getOutput(0), pointMapper.getOutput(0));

		module.getOutput(0).setInputs(unionAll.getOutputs());

		return module;
	}

}
