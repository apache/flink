/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.io;

import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * The OutputSchemaProvider interface can be implemented by {@link InputFormat}.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public interface OutputSchemaProvider {

	/**
	 * Returns a sorted array with the field indexes that are set in the {@link PactRecord}s 
	 * emitted by the {@link InputFormat}.
	 * 
	 * @return a <b>sorted</b> array of all field indexes that are set on output {@link PactRecord}s.
	 */
	public int[] getOutputSchema();
	
}
