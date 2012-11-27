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

package eu.stratosphere.nephele.executiongraph;

/**
 * A graph conversion exception is thrown if the creation of transformation
 * of an {@link ExecutionGraph} fails.
 * 
 * @author warneke
 */
public class GraphConversionException extends Exception {

	/**
	 * Generated serial version UID.
	 */
	private static final long serialVersionUID = -7623370680208569211L;

	/**
	 * Creates a new exception with the given error message.
	 * 
	 * @param msg
	 *        the error message to be transported through this exception
	 */
	public GraphConversionException(String msg) {
		super(msg);
	}

}
