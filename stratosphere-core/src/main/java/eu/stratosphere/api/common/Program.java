/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common;

import java.io.Serializable;

/**
 * A Program represents and end-to-end Stratosphere program. The Program creates the {@link Plan}, which describes
 * the data flow to be executed.
 */
public interface Program extends Serializable {
	
	/**
	 * The method which is invoked by the compiler to get the job that is then compiled into an
	 * executable schedule.
	 * 
	 * @param args The array of input parameters. The parameters may be taken from the command line.
	 * 
	 * @return The job to be compiled and executed.
	 */
	Plan getPlan(String... args);
}
