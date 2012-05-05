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
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;

/**
 * @author skruse
 * 
 */
public class Clustering extends CompositeOperator<Clustering> {

	private static final long serialVersionUID = -747074302410053877L;

	public static final int SAMPLE_INPUT_INDEX = 0;
	public static final int REST_INPUT_INDEX = 1;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 2, 4);

		final Operator<?> sampleInput = module.getInput(SAMPLE_INPUT_INDEX);
		final Operator<?> restInput = module.getInput(REST_INPUT_INDEX);

		SimpleClustering simpleClustering = new SimpleClustering().withInputs(sampleInput, restInput);
		
		// TODO PostProcessing

		return module;
	}

}
