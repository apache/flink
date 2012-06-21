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
package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Value;

/**
 * A FieldParser is used parse a field and fill a {@link Value} from a sequence of bytes.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <T> The type of {@link Value} that is filled.
 */
public interface FieldParser<T extends Value> {
	
	/**
	 * Configures the parser.
	 * 
	 * @param config The configuration of the InputFormat.
	 */
	public void configure(Configuration config);
	
	/**
	 * Fills a given {@link Value} with the value of a field by parsing a byte array. 
	 * The start position within the byte array and the array's valid length is given. 
	 * The content of the Value is delimited by a field delimiter.
	 * 
	 * @param bytes The byte array that holds the value.
	 * @param startPos The index where the field starts
	 * @param limit The limit unto which the byte contents is valid for the parser. The limit is the
	 *              position one after the last valid byte.
	 * @param delim Field delimiter character
	 * @param field The field to hold the value
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	public int parseField(byte[] bytes, int startPos, int limit, char delim, T field);
	
	/**
	 * Returns an instance of the parsed value type.
	 * 
	 * @return An instance of the parsed value type. 
	 */
	public T getValue();
	
}