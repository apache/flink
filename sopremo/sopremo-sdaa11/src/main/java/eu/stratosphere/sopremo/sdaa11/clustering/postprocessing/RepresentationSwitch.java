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
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.clustering.main.ClusterRepresentation;

/**
 * Inputs:
 * <ol>
 * <li>Updated cluster representations</li>
 * </ol>
 * Outputs:
 * <ol>
 * <li>Stable cluster representations</li>
 * <li>Unstable cluster representations</li>
 * <li>Split cluster representations</li>
 * </ol>
 * 
 * 
 * @author skruse
 * 
 * 
 */
@OutputCardinality(min = 3, max = 3)
@Name(noun = "Representation Switch")
public class RepresentationSwitch extends
		CompositeOperator<RepresentationSwitch> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7105199259028835295L;

	public RepresentationSwitch() {
		super(3);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new ElementarySopremoModule(
				this.getName(), 1, 3);

		this.addSelection(module, ClusterRepresentation.STABLE_FLAG, 0);
		this.addSelection(module, ClusterRepresentation.UNSTABLE_FLAG, 1);
		this.addSelection(module, ClusterRepresentation.SPLIT_FLAG, 2);

		return module.asElementary();
	}

	private void addSelection(final SopremoModule module, final int flag,
			final int outputNumber) {
		final Source input = module.getInput(0);
		final RepresentationSelection representationSelection = new RepresentationSelection()
				.withInputs(input);
		representationSelection.setFlag(flag);
		module.getOutput(outputNumber).setInput(0, representationSelection);
	}

}
