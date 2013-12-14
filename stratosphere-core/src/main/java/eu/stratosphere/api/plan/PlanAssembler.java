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

package eu.stratosphere.api.plan;

/**
 * Interface that shows that the class contains the method to generate the plan for a Pact Program.
 */
public interface PlanAssembler
{
	/**
	 * The method which is invoked by the Pact compiler to get the program's plan, that is then compiled into an
	 * executable schedule.
	 * 
	 * @param args The array of input parameters. Those are taken from the command line or from teh parameter line in the
	 *             web frontend.
	 *              
	 * @return The plan to be compiled and executed.
	 */
	Plan getPlan(String... args);
}
