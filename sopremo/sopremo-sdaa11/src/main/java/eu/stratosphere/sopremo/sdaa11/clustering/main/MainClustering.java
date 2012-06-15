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
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
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
@InputCardinality(value = 4)
@OutputCardinality(value = 2)
public class MainClustering extends CompositeOperator<MainClustering> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7712289910767829747L;
	private int representationDetail = RepresentationUpdate.DEFAULT_REPRESENTATION_DETAIL;
	private int maxClusterRadius = RepresentationUpdate.DEFAULT_MAX_CLUSTER_RADIUS;
	private int maxClustroidShift = RepresentationUpdate.DEFAULT_MAX_CLUSTROID_SHIFT;
	private int minPointCount = RepresentationUpdate.DEFAULT_MIN_POINT_COUNT;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final ElementarySopremoModule module = new ElementarySopremoModule(
				this.getName(), 4, 2);

		final Source initialClustersInput = module.getInput(0);
		final Source restPointsInput = module.getInput(1);
		final Source treeInput = module.getInput(2);
		final Source representationInput = module.getInput(3);

		final ClusterDisassemble disassemble = new ClusterDisassemble()
				.withInputs(initialClustersInput);

		final PointMapper pointMapper = new PointMapper().withInputs(restPointsInput, treeInput);

		final UnionAll pointUnionAll = new UnionAll().withInputs(disassemble,
				pointMapper);

		final RepresentationUpdate representationUpdate = new RepresentationUpdate()
				.withInputs(representationInput, pointUnionAll);
		representationUpdate.setMaxClusterRadius(this.maxClusterRadius);
		representationUpdate.setMinPointCount(this.minPointCount);
		representationUpdate.setMaxClustroidShift(this.maxClustroidShift);
		representationUpdate.setRepresentationDetail(this.representationDetail);
		
		module.getOutput(0).setInputs(pointUnionAll);
		module.getOutput(1).setInputs(representationUpdate);

		return module;
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

	/**
	 * Returns the maxClusterRadius.
	 * 
	 * @return the maxClusterRadius
	 */
	public int getMaxClusterRadius() {
		return this.maxClusterRadius;
	}

	/**
	 * Sets the maxClusterRadius to the specified value.
	 * 
	 * @param maxClusterRadius
	 *            the maxClusterRadius to set
	 */
	public void setMaxClusterRadius(final int maxClusterRadius) {
		this.maxClusterRadius = maxClusterRadius;
	}

	/**
	 * Returns the maxClustroidShift.
	 * 
	 * @return the maxClustroidShift
	 */
	public int getMaxClustroidShift() {
		return this.maxClustroidShift;
	}

	/**
	 * Sets the maxClustroidShift to the specified value.
	 * 
	 * @param maxClustroidShift
	 *            the maxClustroidShift to set
	 */
	public void setMaxClustroidShift(final int maxClustroidShift) {
		this.maxClustroidShift = maxClustroidShift;
	}

	/**
	 * Returns the minPointCount.
	 * 
	 * @return the minPointCount
	 */
	public int getMinPointCount() {
		return this.minPointCount;
	}

	/**
	 * Sets the minPointCount to the specified value.
	 * 
	 * @param minPointCount
	 *            the minPointCount to set
	 */
	public void setMinPointCount(final int minPointCount) {
		this.minPointCount = minPointCount;
	}

}
