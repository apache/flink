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
package eu.stratosphere.sopremo.sdaa11.clustering;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.SequentialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.main.RepresentationUpdate;
import eu.stratosphere.sopremo.sdaa11.clustering.postprocessing.PostProcess;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeAssembler;

/**
 * 
 * Inputs:
 * <ol>
 * <li>Sample points</li>
 * <li>Remaining points</li>
 * </ol>
 * Outputs: See {@link PostProcess}
 * 
 * @author skruse
 * 
 */
@InputCardinality(value = 2)
@OutputCardinality(value = 4)
public class Clustering extends CompositeOperator<Clustering> {

	private static final long serialVersionUID = -747074302410053877L;

	public static final int SAMPLE_INPUT_INDEX = 0;
	public static final int REST_INPUT_INDEX = 1;

	private int maxInitialClusterSize = SequentialClustering.DEFAULT_MAX_SIZE;
	private int maxInitialClusterRadius = SequentialClustering.DEFAULT_MAX_RADIUS;
	private int treeWidth = TreeAssembler.DEFAULT_TREE_WIDTH;
	private int representationDetail = RepresentationUpdate.DEFAULT_REPRESENTATION_DETAIL;
	private int maxFinalClusterRadius = RepresentationUpdate.DEFAULT_MAX_CLUSTER_RADIUS;
	private int maxClustroidShift = RepresentationUpdate.DEFAULT_MAX_CLUSTROID_SHIFT;
	private int minPointCount = RepresentationUpdate.DEFAULT_MIN_POINT_COUNT;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 2, 4);

		final Operator<?> sampleInput = module.getInput(SAMPLE_INPUT_INDEX);
		final Operator<?> restInput = module.getInput(REST_INPUT_INDEX);

		final SimpleClustering simpleClustering = new SimpleClustering()
				.withInputs(sampleInput, restInput);
		simpleClustering.setMaxClustroidShift(this.maxClustroidShift);
		simpleClustering.setMaxFinalClusterRadius(this.maxFinalClusterRadius);
		simpleClustering
				.setMaxInitialClusterRadius(this.maxInitialClusterRadius);
		simpleClustering.setMaxInitialClusterSize(this.maxInitialClusterSize);
		simpleClustering.setMinPointCount(this.minPointCount);
		simpleClustering.setRepresentationDetail(this.representationDetail);
		simpleClustering.setTreeWidth(this.treeWidth);

		final PostProcess postProcess = new PostProcess()
				.withInputs(simpleClustering.getOutputs());
		postProcess.setRepresentationDetail(this.representationDetail);

		for (int index = 0; index < 4; index++)
			module.getOutput(index).setInput(0, postProcess.getOutput(index));

		return module.asElementary();
	}

	/**
	 * Returns the maxInitialClusterSize.
	 * 
	 * @return the maxInitialClusterSize
	 */
	public int getMaxInitialClusterSize() {
		return this.maxInitialClusterSize;
	}

	/**
	 * Sets the maxInitialClusterSize to the specified value.
	 * 
	 * @param maxInitialClusterSize
	 *            the maxInitialClusterSize to set
	 */
	public void setMaxInitialClusterSize(final int maxInitialClusterSize) {
		this.maxInitialClusterSize = maxInitialClusterSize;
	}

	/**
	 * Returns the maxInitialClusterRadius.
	 * 
	 * @return the maxInitialClusterRadius
	 */
	public int getMaxInitialClusterRadius() {
		return this.maxInitialClusterRadius;
	}

	/**
	 * Sets the maxInitialClusterRadius to the specified value.
	 * 
	 * @param maxInitialClusterRadius
	 *            the maxInitialClusterRadius to set
	 */
	public void setMaxInitialClusterRadius(final int maxInitialClusterRadius) {
		this.maxInitialClusterRadius = maxInitialClusterRadius;
	}

	/**
	 * Returns the treeWidth.
	 * 
	 * @return the treeWidth
	 */
	public int getTreeWidth() {
		return this.treeWidth;
	}

	/**
	 * Sets the treeWidth to the specified value.
	 * 
	 * @param treeWidth
	 *            the treeWidth to set
	 */
	public void setTreeWidth(final int treeWidth) {
		this.treeWidth = treeWidth;
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
	 * Returns the maxFinalClusterRadius.
	 * 
	 * @return the maxFinalClusterRadius
	 */
	public int getMaxFinalClusterRadius() {
		return this.maxFinalClusterRadius;
	}

	/**
	 * Sets the maxFinalClusterRadius to the specified value.
	 * 
	 * @param maxFinalClusterRadius
	 *            the maxFinalClusterRadius to set
	 */
	public void setMaxFinalClusterRadius(final int maxFinalClusterRadius) {
		this.maxFinalClusterRadius = maxFinalClusterRadius;
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
