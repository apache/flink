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
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
@OutputCardinality(min = 4, max = 4)
public class Clustering extends CompositeOperator<Clustering> {

	private static final long serialVersionUID = -747074302410053877L;

	public static final int SAMPLE_INPUT_INDEX = 0;
	public static final int REST_INPUT_INDEX = 1;

	private final int maxInitialClusterSize = SequentialClustering.DEFAULT_MAX_SIZE;
	private final int maxInitialClusterRadius = SequentialClustering.DEFAULT_MAX_RADIUS;
	private final int treeWidth = TreeAssembler.DEFAULT_TREE_WIDTH;
	private final int representationDetail = RepresentationUpdate.DEFAULT_REPRESENTATION_DETAIL;
	private final int maxFinalClusterRadius = RepresentationUpdate.DEFAULT_MAX_CLUSTER_RADIUS;
	private final int maxClustroidShift = RepresentationUpdate.DEFAULT_MAX_CLUSTROID_SHIFT;
	private final int minPointCount = RepresentationUpdate.DEFAULT_MIN_POINT_COUNT;

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

		final PostProcess postProcess = new PostProcess()
				.withInputs(simpleClustering.getOutputs());

		for (int index = 0; index < 4; index++)
			module.getOutput(index).setInput(0, postProcess.getOutput(index));

		return module.asElementary();
	}

}
