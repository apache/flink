/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.types.parser;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

/**
 * A FieldParser is used parse a field from a sequence of bytes. Fields occur in a byte sequence and are terminated
 * by the end of the byte sequence or a delimiter.
 * <p>
 * The parsers do not throw exceptions in general, but set an error state. That way, they can be used in functions
 * that ignore invalid lines, rather than failing on them.
 *
 * @param <T> The type that is parsed.
 */
public abstract class FieldParser<T> {
	
	/**
	 * An enumeration of different types of errors that may occur.
	 */
	public static enum ParseErrorState {
		/** No error occurred. */
		NONE,

		/** The domain of the numeric type is not large enough to hold the parsed value. */
		NUMERIC_VALUE_OVERFLOW_UNDERFLOW,

		/** A stand-alone sign was encountered while parsing a numeric type. */
		NUMERIC_VALUE_ORPHAN_SIGN,

		/** An illegal character was encountered while parsing a numeric type. */
		NUMERIC_VALUE_ILLEGAL_CHARACTER,

		/** The field was not in a correct format for the numeric type. */
		NUMERIC_VALUE_FORMAT_ERROR,

		/** A quoted string was not terminated until the line end. */
		UNTERMINATED_QUOTED_STRING,

		/** The parser found characters between the end of the quoted string and the delimiter. */
		UNQUOTED_CHARS_AFTER_QUOTED_STRING,

		/** The string is empty. */
		EMPTY_STRING,

		/** Invalid Boolean value **/
		BOOLEAN_INVALID
	}
	
	private ParseErrorState errorState = ParseErrorState.NONE;
	
	/**
	 * Parses the value of a field from the byte array.
	 * The start position within the byte array and the array's valid length is given. 
	 * The content of the value is delimited by a field delimiter.
	 * 
	 * @param bytes The byte array that holds the value.
	 * @param startPos The index where the field starts
	 * @param limit The limit unto which the byte contents is valid for the parser. The limit is the
	 *              position one after the last valid byte.
	 * @param delim The field delimiter character
	 * @param reuse An optional reusable field to hold the value
	 * 
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	public abstract int parseField(byte[] bytes, int startPos, int limit, byte[] delim, T reuse);
	
	/**
	 * Gets the parsed field. This method returns the value parsed by the last successful invocation of
	 * {@link #parseField(byte[], int, int, byte[], Object)}. It objects are mutable and reused, it will return
	 * the object instance that was passed the parse function.
	 * 
	 * @return The latest parsed field.
	 */
	public abstract T getLastResult();
	
	/**
	 * Returns an instance of the parsed value type.
	 * 
	 * @return An instance of the parsed value type. 
	 */
	public abstract T createValue();
	
	/**
	 * Checks if the delimiter starts at the given start position of the byte array.
	 * 
	 * Attention: This method assumes that enough characters follow the start position for the delimiter check!
	 * 
	 * @param bytes The byte array that holds the value.
	 * @param startPos The index of the byte array where the check for the delimiter starts.
	 * @param delim The delimiter to check for.
	 * 
	 * @return true if a delimiter starts at the given start position, false otherwise.
	 */
	public static final boolean delimiterNext(byte[] bytes, int startPos, byte[] delim) {

		for(int pos = 0; pos < delim.length; pos++) {
			// check each position
			if(delim[pos] != bytes[startPos+pos]) {
				return false;
			}
		}
		return true;
		
	}
	
	/**
	 * Sets the error state of the parser. Called by subclasses of the parser to set the type of error
	 * when failing a parse.
	 * 
	 * @param error The error state to set.
	 */
	protected void setErrorState(ParseErrorState error) {
		this.errorState = error;
	}
	
	/**
	 * Gets the error state of the parser, as a value of the enumeration {@link ParseErrorState}.
	 * If no error occurred, the error state will be {@link ParseErrorState#NONE}.
	 * 
	 * @return The current error state of the parser.
	 */
	public ParseErrorState getErrorState() {
		return this.errorState;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Mapping from types to parsers
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the parser for the type specified by the given class. Returns null, if no parser for that class
	 * is known.
	 * 
	 * @param type The class of the type to get the parser for.
	 * @return The parser for the given type, or null, if no such parser exists.
	 */
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
		PARSERS.put(String.class, StringParser.class);
		PARSERS.put(Float.class, FloatParser.class);
		PARSERS.put(Double.class, DoubleParser.class);
		PARSERS.put(Boolean.class, BooleanParser.class);

		// value types
		PARSERS.put(ByteValue.class, ByteValueParser.class);
		PARSERS.put(ShortValue.class, ShortValueParser.class);
		PARSERS.put(IntValue.class, IntValueParser.class);
		PARSERS.put(LongValue.class, LongValueParser.class);
		PARSERS.put(StringValue.class, StringValueParser.class);
		PARSERS.put(FloatValue.class, FloatValueParser.class);
		PARSERS.put(DoubleValue.class, DoubleValueParser.class);
		PARSERS.put(BooleanValue.class, BooleanValueParser.class);
	}
}
