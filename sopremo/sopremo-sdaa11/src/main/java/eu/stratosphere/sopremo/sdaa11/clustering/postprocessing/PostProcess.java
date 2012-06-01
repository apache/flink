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
package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;

/**
 * Post-processes a clustering result, i.e. do splitting and separate unstable from stable clusters.<br>
 * Inputs:<br>
 * <ol>
 * <li>Assigned points</li>
 * <li>Cluster representations</li>
 * </ol>
 * Outputs:<br>
 * <ol>
 * <li>Stable cluster representations</li>
 * <li>Assigned points of stable clusters</li>
 * <li>Unstable cluster representations</li>
 * <li>Assigned points of unstable clusters</li>
 * </ol>
 * 
 * 
 * @author skruse
 * 
 */
public class PostProcess extends CompositeOperator<PostProcess> {

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		SopremoModule module = new SopremoModule(getName(), 2, 4);
		Source pointSource = module.getInput(0);
		Source representationSource = module.getInput(1);
		
		RepresentationSwitch representationSwitch = new RepresentationSwitch().withInputs(representationSource);
		JsonStream stablePoints = representationSwitch.getOutput(0);
		module.getOutput(0).setInput(0, stablePoints);
		
		PointSelection stablePointSelection = new PointSelection().withInputs(stablePoints);
		module.getOutput(1).setInput(0, stablePointSelection);
		
		return module.asElementary();
	}

}
