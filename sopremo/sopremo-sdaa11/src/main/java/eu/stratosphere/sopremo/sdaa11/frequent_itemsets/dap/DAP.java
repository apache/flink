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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap;

import org.apache.commons.lang.NotImplementedException;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;

/**
 * @author skruse
 * 
 */
public class DAP extends CompositeOperator<DAP> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6149175366611975804L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators() {
		throw new NotImplementedException();
	}

}
