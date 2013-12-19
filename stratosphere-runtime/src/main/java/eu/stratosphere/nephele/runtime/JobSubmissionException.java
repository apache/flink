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

package eu.stratosphere.nephele.runtime;

/**
 * A job submission exception is thrown if an error occurs while submitting
 * a job from the client to the job manager.
 * 
 */
public class JobSubmissionException extends Exception {

	/**
	 * Generated serial UID
	 */
	private static final long serialVersionUID = 1275864691743020176L;

	/**
	 * Constructs a new job submission exception with the given error message.
	 * 
	 * @param msg
	 *        the error message to be transported through this exception
	 */
	public JobSubmissionException(String msg) {
		super(msg);
	}

}
