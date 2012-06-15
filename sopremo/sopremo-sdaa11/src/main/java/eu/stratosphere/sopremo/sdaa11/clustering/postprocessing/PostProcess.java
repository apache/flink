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

import temp.UnionAll;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.clustering.main.RepresentationUpdate;

/**
 * Post-processes a clustering result, i.e. do splitting and separate unstable
 * from stable clusters.<br>
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
@InputCardinality(value = 2)
@OutputCardinality(value = 4)
public class PostProcess extends CompositeOperator<PostProcess> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1517888414053098096L;

	private int representationDetail = RepresentationUpdate.DEFAULT_REPRESENTATION_DETAIL;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 2, 4);
		final Source pointSource = module.getInput(0);
		final Source representationSource = module.getInput(1);

		final RepresentationSwitch representationSwitch = new RepresentationSwitch()
				.withInputs(representationSource);
		final JsonStream stableRepresentations = representationSwitch
				.getOutput(0);
		final JsonStream unstableRepresentations = representationSwitch
				.getOutput(1);
		final JsonStream splitRepresentations = representationSwitch
				.getOutput(2);

		module.getOutput(0).setInput(0, stableRepresentations);

		final PointSelection stablePointSelection = new PointSelection()
				.withInputs(pointSource, stableRepresentations);
		module.getOutput(1).setInput(0, stablePointSelection);

		final Split split = new Split().withInputs(splitRepresentations,
				pointSource);
		split.setRepresentationDetail(this.representationDetail);
		final UnionAll priorReclusterRepresentations = new UnionAll()
				.withInputs(unstableRepresentations, split);
		module.getOutput(2).setInputs(priorReclusterRepresentations);

		final CanonicalizeRepresentations canonicalizeRepresentations = new CanonicalizeRepresentations()
				.withInputs(splitRepresentations);
		final UnionAll reclusterRepresentations = new UnionAll().withInputs(
				unstableRepresentations, canonicalizeRepresentations);
		final PointSelection unstablePointSelection = new PointSelection()
				.withInputs(pointSource, reclusterRepresentations);
		module.getOutput(3).setInput(0, unstablePointSelection);

		return module.asElementary();
	}

	/**
	 * Returns the representationDetail.
	 * 
	 * @return the representationDetail
	 */
	public int getRepresentationDetail() {
		return this.representationDetail;
	}

	/**
	 * Sets the representationDetail to the specified value.
	 * 
	 * @param representationDetail
	 *            the representationDetail to set
	 */
	public void setRepresentationDetail(final int representationDetail) {
		this.representationDetail = representationDetail;
	}

}
