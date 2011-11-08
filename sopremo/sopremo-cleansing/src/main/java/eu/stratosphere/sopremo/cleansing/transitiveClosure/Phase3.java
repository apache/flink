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
package eu.stratosphere.sopremo.cleansing.transitiveClosure;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.SopremoModule;


public class Phase3 extends CompositeOperator<Phase3>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5629802867631098519L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {		

		final SopremoModule sopremoModule = new SopremoModule(this.getName(), 2, 1);
		JsonStream phase1 = sopremoModule.getInput(0);
		JsonStream matrix = sopremoModule.getInput(1);

		

//		final GenerateColumns columns = new GenerateColumns().withInputs(computeRows);
//		final ComputeBlockTuples computeTuples = new ComputeBlockTuples().withInputs(transDia, columns);

//		sopremoModule.getOutput(0).setInput(0, /*lastOperator*/);

		return sopremoModule;
	}

}
