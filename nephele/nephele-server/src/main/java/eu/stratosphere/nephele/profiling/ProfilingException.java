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

package eu.stratosphere.nephele.profiling;

/**
 * A profiling exception is thrown if an error occur during profiling execution.
 * 
 * @author Alexander Stanik
 */
public class ProfilingException extends Exception {

	/**
	 * Generated serialVersionUID.
	 */
	private static final long serialVersionUID = -3282996556813630561L;

	/**
	 * Constructs a new profiling exception with the given error message.
	 * 
	 * @param errorMsg
	 *        The error message to be included in the exception.
	 */
	public ProfilingException(String errorMsg) {
		super(errorMsg);
	}
}
