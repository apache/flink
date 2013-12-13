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

package eu.stratosphere.nephele.jobmanager.scheduler;

/**
 * Scheduling exceptions are thrown to indicate problems or errors
 * related to Nephele's scheduler.
 * 
 * @author warneke
 */
public class SchedulingException extends Exception {

	/**
	 * Generated serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new scheduling exception object.
	 * 
	 * @param msg
	 *        the error message of the exception
	 */
	public SchedulingException(String msg) {
		super(msg);
	}

}
