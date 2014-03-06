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

import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * A FieldParser is used parse a field and fill a {@link Value} from a sequence of bytes.
 *
 * @param <T> The type that is parsed.
 */
public abstract class FieldParser<T> {
	
	public static enum ParseErrorState {
		NONE,
		NUMERIC_VALUE_OVERFLOW_UNDERFLOW,
		NUMERIC_VALUE_ORPHAN_SIGN,
		NUMERIC_VALUE_ILLEGAL_CHARACTER,
		UNTERMINATED_QUOTED_STRING,
		UNQUOTED_CHARS_AFTER_QUOTED_STRING
	}
	
	private ParseErrorState errorState = ParseErrorState.NONE;
	
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
	 * @param reuse The an optional reusable field to hold the value
	 * 
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	public abstract int parseField(byte[] bytes, int startPos, int limit, char delim, T reuse);
	
	public abstract T getLastResult();
	
	/**
	 * Returns an instance of the parsed value type.
	 * 
	 * @return An instance of the parsed value type. 
	 */
	public abstract T createValue();
	
	
	
	protected void setErrorState(ParseErrorState error) {
		this.errorState = error;
	}
	
	public ParseErrorState getErrorState() {
		return this.errorState;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Mapping from types to parsers
	// --------------------------------------------------------------------------------------------
	
	public static <T> Class<FieldParser<T>> getParserForType(Class<T> type) {
		Class<? extends FieldParser<?>> parser = PARSERS.get(type);
		if (parser == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			Class<FieldParser<T>> typedParser = (Class<FieldParser<T>>) parser;
			return typedParser;
		}
	}
	
	private static final Map<Class<?>, Class<? extends FieldParser<?>>> PARSERS = 
			new HashMap<Class<?>, Class<? extends FieldParser<?>>>();
	
	static {
		// basic types
		PARSERS.put(Byte.class, ByteParser.class);
		PARSERS.put(Short.class, ShortParser.class);
		PARSERS.put(Integer.class, IntParser.class);
		PARSERS.put(Long.class, LongParser.class);
		PARSERS.put(String.class, AsciiStringParser.class);
		PARSERS.put(Float.class, FloatParser.class);
		PARSERS.put(Double.class, DoubleParser.class);
		
		// value types
		PARSERS.put(ByteValue.class, DecimalTextByteParser.class);
		PARSERS.put(ShortValue.class, DecimalTextShortParser.class);
		PARSERS.put(IntValue.class, DecimalTextIntParser.class);
		PARSERS.put(LongValue.class, DecimalTextLongParser.class);
		PARSERS.put(StringValue.class, VarLengthStringParser.class);
		PARSERS.put(FloatValue.class, DecimalTextFloatParser.class);
		PARSERS.put(DoubleValue.class, DecimalTextDoubleParser.class);
	}
}