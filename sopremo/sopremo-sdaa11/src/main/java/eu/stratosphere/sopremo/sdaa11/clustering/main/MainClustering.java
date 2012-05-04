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
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;

/**
 * @author skruse
 *
 */
@InputCardinality(min = 4, max = 4)
public class MainClustering extends CompositeOperator<MainClustering> {

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(getName(), 4, 2);
		
		Source initialClustersInput = module.getInput(0);
		Source restPointsInput = module.getInput(1);
		Source treeInput = module.getInput(2);
		Source representationInput = module.getInput(3);
		
		ClusterDisassemble disassemble = new ClusterDisassemble().withInputs(initialClustersInput);
		
		PointMapper pointMapper = new PointMapper();
		pointMapper.setInput(PointMapper.POINT_INPUT_INDEX, restPointsInput);
		pointMapper.setInput(PointMapper.TREE_INPUT_INDEX, treeInput);
		
		UnionAll pointUnionAll = new UnionAll().withInputs(disassemble, pointMapper);
		
		RepresentationUpdate representationUpdate = new RepresentationUpdate().withInputs(representationInput, pointUnionAll);
		
		module.getOutput(0).setInput(0, pointUnionAll);
		module.getOutput(1).setInput(0, representationUpdate);
		
		return module;
	}

}
