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

package eu.stratosphere.types.parser;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.types.PactByte;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactFloat;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactShort;
import eu.stratosphere.types.PactString;
import eu.stratosphere.types.Value;

/**
 * A FieldParser is used parse a field and fill a {@link Value} from a sequence of bytes.
 *
 * @param <T> The type of {@link Value} that is filled.
 */
public abstract class FieldParser<T extends Value> {
	
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
	public abstract int parseField(byte[] bytes, int startPos, int limit, char delim, T field);
	
	/**
	 * Returns an instance of the parsed value type.
	 * 
	 * @return An instance of the parsed value type. 
	 */
	public abstract T createValue();
	
	
	// --------------------------------------------------------------------------------------------
	//  Mapping from types to parsers
	// --------------------------------------------------------------------------------------------
	
	public static <T extends Value> Class<FieldParser<T>> getParserForType(Class<T> type) {
		Class<? extends FieldParser<?>> parser = PARSERS.get(type);
		if (parser == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			Class<FieldParser<T>> typedParser = (Class<FieldParser<T>>) parser;
			return typedParser;
		}
	}
	
	private static final Map<Class<? extends Value>, Class<? extends FieldParser<?>>> PARSERS = 
			new HashMap<Class<? extends Value>, Class<? extends FieldParser<?>>>();
	
	static {
		PARSERS.put(PactByte.class, DecimalTextByteParser.class);
		PARSERS.put(PactShort.class, DecimalTextShortParser.class);
		PARSERS.put(PactInteger.class, DecimalTextIntParser.class);
		PARSERS.put(PactLong.class, DecimalTextLongParser.class);
		PARSERS.put(PactString.class, VarLengthStringParser.class);
		PARSERS.put(PactFloat.class, DecimalTextFloatParser.class);
		PARSERS.put(PactDouble.class, DecimalTextDoubleParser.class);
	}
}