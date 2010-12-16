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

package eu.stratosphere.nephele.io;

/**
 * This listener interface can be used to obtain information
 * about the utilization of the attached {@link InputGate}.
 * 
 * @author stanik
 */
public interface InputGateListener {

	/**
	 * This method is called by the {@link InputGate} whenever it has to
	 * wait because none of its channels can currently deliver new data.
	 */
	void waitingForAnyChannel();
}
