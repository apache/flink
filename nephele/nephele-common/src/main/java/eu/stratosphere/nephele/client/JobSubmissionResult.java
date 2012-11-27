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

package eu.stratosphere.nephele.client;

/**
 * A job submission result is used to report the results of a job submission. It contains a return code and a
 * description. In case of a submission error the description includes an error message.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class JobSubmissionResult extends AbstractJobResult {

	/**
	 * Constructs a new job submission result object with the given return code.
	 * 
	 * @param returnCode
	 *        the return code of the submission result
	 * @param description
	 *        the error description
	 */
	public JobSubmissionResult(final ReturnCode returnCode, final String description) {
		super(returnCode, description);
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private JobSubmissionResult() {
	}
}
