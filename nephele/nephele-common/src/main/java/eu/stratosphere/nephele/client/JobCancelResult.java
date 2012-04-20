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

package eu.stratosphere.nephele.client;

/**
 * A <code>JobCancelResult</code> is used to report the result
 * of a job cancel request. It contains a return code and a description.
 * In case of a submission error the description includes an error message.
 * 
 * @author warneke
 */
public class JobCancelResult extends AbstractJobResult {

	/**
	 * Constructs a new <code>JobCancelResult</code> object with
	 * the given return code.
	 * 
	 * @param returnCode
	 *        the return code of the submission result.
	 * @param description
	 *        the optional error description
	 */
	public JobCancelResult(final ReturnCode returnCode, final String description) {
		super(returnCode, description);
	}

	/**
	 * Constructs an empty <code>JobCancelResult</code> object.
	 * This constructor is used for object deserialization only and
	 * should not be called.
	 */
	public JobCancelResult() {
		super();
	}
}
