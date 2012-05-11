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
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;

/**
 * Inputs:<br>
 * <ol>
 * <li>Initial clusters</li>
 * <li>Remaining points</li>
 * <li>Cluster tree</li>
 * <li>Cluster representations</li>
 * </ol>
 * Ouputs:<br>
 * <ol>
 * <li>Assigned points</li>
 * <li>Cluster representations</li>
 * </ol>
 * 
 * 
 * 
 * @author skruse
 * 
 */
@InputCardinality(min = 4, max = 4)
@OutputCardinality(min = 2, max = 2)
public class MainClustering extends CompositeOperator<MainClustering> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7712289910767829747L;

	public MainClustering() {
		super(2);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 4, 2);

		final Source initialClustersInput = module.getInput(0);
		final Source restPointsInput = module.getInput(1);
		final Source treeInput = module.getInput(2);
		final Source representationInput = module.getInput(3);

		final ClusterDisassemble disassemble = new ClusterDisassemble()
				.withInputs(initialClustersInput);

		final PointMapper pointMapper = new PointMapper();
		pointMapper.setInput(PointMapper.POINT_INPUT_INDEX, restPointsInput);
		pointMapper.setInput(PointMapper.TREE_INPUT_INDEX, treeInput);

		final UnionAll pointUnionAll = new UnionAll().withInputs(disassemble,
				pointMapper);

		final RepresentationUpdate representationUpdate = new RepresentationUpdate()
				.withInputs(representationInput, pointUnionAll);

		module.getOutput(0).setInputs(pointUnionAll);
		module.getOutput(1).setInputs(representationUpdate);

		return module;
	}

}
