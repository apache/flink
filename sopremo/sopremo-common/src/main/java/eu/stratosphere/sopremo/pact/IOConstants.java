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
package eu.stratosphere.sopremo.pact;

/**
 * Defines constants that are used by {@link InputFormat}s to read there data
 * 
 * @author Arvid Heise
 */
public interface IOConstants {
	
	/**
	 * Constant to define which encoding is used
	 */
	public static final String ENCODING = "Encoding";
	
	/**
	 * Constant to define which {@link Schema} is used
	 */
	public static final String SCHEMA = "targetSchema";

	/**
	 * Constant to define the column names
	 */
	public static final String COLUMN_NAMES = "columnNames";

	/**
	 * Constant to define which field delimiter is used
	 */
	public static final String FIELD_DELIMITER = "fieldDelimiter";
}
