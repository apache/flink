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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.RoundRobinAnnotator;

/**
 * @author skruse
 * 
 */
public class SON extends CompositeOperator<SON> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4600530033164134202L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

		final Source basketSource = module.getInput(0);
		final RoundRobinAnnotator annotator = new RoundRobinAnnotator()
				.withInputs(basketSource);
		annotator.setMaxAnnotation(10);

		final FindUnaryFrequentItemSets unaryItemSets = new FindUnaryFrequentItemSets()
				.withInputs(annotator);

		module.getOutput(0).setInputs(unaryItemSets);

		return module.asElementary();
	}

}
