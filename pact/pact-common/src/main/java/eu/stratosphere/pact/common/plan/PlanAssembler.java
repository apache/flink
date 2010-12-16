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

package eu.stratosphere.pact.common.plan;

/**
 * Interface that shows that the class contains the method to generate the input
 * plan.
 * 
 * @author DIMA
 */
public interface PlanAssembler {
	/**
	 * The method which is invoked by the nephele PACs engine in order to get the
	 * input plan.
	 * 
	 * @param args
	 *        array of input parameters
	 * @return the plan to be compiled and executed
	 * @throws IllegalArgumentException
	 *         if an input parameter is missing or illegal
	 */
	Plan getPlan(String... args) throws IllegalArgumentException;
}
