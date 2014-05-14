/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils;

/**
 * {@link eu.stratosphere.api.common.io.OutputFormat}s can implement this interface to be configured
 * with the data type they will operate on. The method {@link #setInputType(TypeInformation)} will be
 * called when the output format is used with an output method such as
 * {@link eu.stratosphere.api.java.DataSet#output(eu.stratosphere.api.common.io.OutputFormat)}.
 */
public interface InputTypeConfigurable {

	/**
	 * Method that is called on an {@link eu.stratosphere.api.common.io.OutputFormat} when it is passed to
	 * the DataSet's output method. May be used to configures the output format based on the data type.
	 * 
	 * @param type The data type of the input.
	 */
	void setInputType(TypeInformation<?> type);
}
