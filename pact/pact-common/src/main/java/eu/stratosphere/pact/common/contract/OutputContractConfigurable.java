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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.Annotation;

/**
 * Interface that indicates that the output contract for a Contract
 * can also be set by using the setOutputContract method instead
 * of annotations.
 * 
 * @author Moritz Kaufmann
 */
public interface OutputContractConfigurable {

	/**
	 * The output contract for the contract
	 * 
	 * @param clazz The class of the OutputContract that is attached.
	 */
	public void setOutputContract(Class<? extends Annotation> clazz);

	/**
	 * Returns the output contract that was set.
	 * 
	 * @return The class of the attached OutputContract.
	 */
	public Class<? extends Annotation> getOutputContract();

}
