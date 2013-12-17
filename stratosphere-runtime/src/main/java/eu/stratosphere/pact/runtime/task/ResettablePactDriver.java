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

package eu.stratosphere.pact.runtime.task;

import eu.stratosphere.api.functions.Function;


/**
 * This interface marks a {@code PactDriver} as resettable, meaning that will reset part of their internal state but
 * otherwise reuse existing data structures.
 *
 * @see PactDriver
 * @see PactTaskContext
 * 
 * @param <S> The type of stub driven by this driver.
 * @param <OT> The data type of the records produced by this driver.
 */
public interface ResettablePactDriver<S extends Function, OT> extends PactDriver<S, OT> {
	
	boolean isInputResettable(int inputNum);
	
	void initialize() throws Exception;
	
	void reset() throws Exception;
	
	void teardown() throws Exception;
}
