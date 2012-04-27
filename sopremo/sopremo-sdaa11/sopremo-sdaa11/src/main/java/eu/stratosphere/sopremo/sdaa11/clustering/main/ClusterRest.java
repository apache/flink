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

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.UnionAll;

/**
 * @author skruse
 *
 */
public class ClusterRest extends CompositeOperator<ClusterRest> {
	
	public static final int SAMPLE_CLUSTERS_INPUT_INDEX = 0;
	public static final int REST_POINTS_INPUT_INDEX = 1;
	public static final int TREE_INPUT_INDEX = 2;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(getName(), 3, 1);
		
		Operator<?> sampleClusterInput = module.getInput(SAMPLE_CLUSTERS_INPUT_INDEX);
		Operator<?> restPointsInput = module.getInput(REST_POINTS_INPUT_INDEX);
		Operator<?> treeInput = module.getInput(TREE_INPUT_INDEX);
		
		Operator<?> disassembleClusters = new ClusterDisassemble().withInputs(sampleClusterInput.getOutput(0));
		Operator<?> pointMapper = new PointMapper().withInputs(treeInput.getOutput(0), restPointsInput.getOutput(0));
		Operator<?> unionAll = new UnionAll().withInputs(disassembleClusters.getOutput(0), pointMapper.getOutput(0));
		
		module.getOutput(0).setInputs(unionAll.getOutputs());
		
		return module;
	}

}
