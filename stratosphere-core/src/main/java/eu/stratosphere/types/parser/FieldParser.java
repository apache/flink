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
		UNQUOTED_CHARS_AFTER_QUOTED_STRING
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
	 * @param delim Field delimiter character
	 * @param reuse The an optional reusable field to hold the value
	 * 
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	public abstract int parseField(byte[] bytes, int startPos, int limit, char delim, T reuse);
	
	/**
	 * Gets the parsed field. This method returns the value parsed by the last successful invocation of
	 * {@link #parseField(byte[], int, int, char, Object)}. It objects are mutable and reused, it will return
	 * the object instance that was passed the the parse function.
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
