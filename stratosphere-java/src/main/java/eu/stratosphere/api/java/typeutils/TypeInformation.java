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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.types.Value;

public abstract class TypeInformation<T> {

	public abstract boolean isBasicType();

	public abstract boolean isTupleType();

	public abstract int getArity();

	public abstract Class<T> getTypeClass();

	public abstract boolean isKeyType();

	public abstract TypeSerializer<T> createSerializer();

	// -------------------------------------------------------------------------

	private static final String TUPLE_PACKAGE = "eu.stratosphere.api.java.tuple";
	private static final String VALUE_PACKAGE = "eu.stratosphere.types";

	private static final Pattern tuplePattern = Pattern.compile("^((" + TUPLE_PACKAGE.replaceAll("\\.", "\\\\.") + "\\.)?Tuple[0-9]+)<");
	private static final Pattern basicTypePattern = Pattern
			.compile("^((java\\.lang\\.)?(String|Integer|Byte|Short|Character|Double|Float|Long|Boolean))(,|>|$)");
	private static final Pattern basicType2Pattern = Pattern.compile("^(int|byte|short|char|double|float|long|boolean)(,|>|$)");
	private static final Pattern valueTypePattern = Pattern.compile("^((" + VALUE_PACKAGE.replaceAll("\\.", "\\\\.")
			+ "\\.)?(String|Int|Byte|Short|Char|Double|Float|Long|Boolean|List|Map|Null))Value(,|>|$)");
	private static final Pattern basicArrayTypePattern = Pattern
			.compile("^((java\\.lang\\.)?(String|Integer|Byte|Short|Character|Double|Float|Long|Boolean))\\[\\](,|>|$)");
	private static final Pattern basicArrayType2Pattern = Pattern.compile("^(int|byte|short|char|double|float|long|boolean)\\[\\](,|>|$)");
	private static final Pattern customObjectPattern = Pattern.compile("^([^\\s,>]+)(,|>|$)");

	/**
	 * Generates an instance of <code>TypeInformation</code> by parsing a type
	 * information string. A type information string can contain the following
	 * types:
	 * 
	 * <ul>
	 * 	<li>Basic types such as <code>Integer</code>, <code>String</code>, etc.
	 * 	<li>Basic type arrays such as <code>Integer[]</code>, <code>String[]</code>, etc.
	 * 	<li>Tuple types such as <code>Tuple1&lt;TYPE0&gt;</code>, <code>Tuple2&lt;TYPE0, TYPE1&gt;</code>, etc.</li>
	 * 	<li>Custom types such as <code>org.my.CustomObject</code>, <code>org.my.CustomObject$InnerClass</code>, etc.
	 * 	<li>Custom type arrays such as <code>org.my.CustomObject[]</code>, <code>org.my.CustomObject$InnerClass[]</code>, etc.
	 * 	<li>Value types such as <code>DoubleValue</code>, <code>StringValue</code>, <code>IntegerValue</code>, etc.</li>
	 * </ul>
	 * 
	 * Example: <code>"Tuple2&lt;String,Tuple2&lt;Integer,org.my.MyClass&gt;&gt;"</code>
	 * 
	 * @param infoString type information string to be parsed
	 * @return <code>TypeInformation</code> representation of the string
	 */
	public static TypeInformation<?> parse(String infoString) {

		try {
			if (infoString == null) {
				throw new IllegalArgumentException("String is null.");
			}
			String clearedString = infoString.replaceAll("\\s", "");
			if (clearedString.length() == 0) {
				throw new IllegalArgumentException("String must not be empty.");
			}
			return parse(new StringBuilder(clearedString));
		} catch (Exception e) {
			throw new IllegalArgumentException("String could not be parsed: " + e.getMessage());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static TypeInformation<?> parse(StringBuilder sb) throws ClassNotFoundException {
		String infoString = sb.toString();
		final Matcher tupleMatcher = tuplePattern.matcher(infoString);

		final Matcher basicTypeMatcher = basicTypePattern.matcher(infoString);
		final Matcher basicType2Matcher = basicType2Pattern.matcher(infoString);

		final Matcher valueTypeMatcher = valueTypePattern.matcher(infoString);

		final Matcher basicArrayTypeMatcher = basicArrayTypePattern.matcher(infoString);
		final Matcher basicArrayType2Matcher = basicArrayType2Pattern.matcher(infoString);

		final Matcher customObjectMatcher = customObjectPattern.matcher(infoString);

		if (infoString.length() == 0) {
			return null;
		}

		TypeInformation<?> returnType = null;

		// tuples
		if (tupleMatcher.find()) {
			String className = tupleMatcher.group(1);
			sb.delete(0, className.length() + 1);
			int arity = Integer.parseInt(className.replaceAll("\\D", ""));

			Class<?> clazz = null;
			// check if fully qualified
			if (className.startsWith(TUPLE_PACKAGE)) {
				clazz = Class.forName(className);
			} else {
				clazz = Class.forName(TUPLE_PACKAGE + "." + className);
			}

			TypeInformation<?>[] types = new TypeInformation<?>[arity];
			for (int i = 0; i < arity; i++) {
				types[i] = parse(sb);
				if (types[i] == null) {
					throw new IllegalArgumentException("Tuple arity does not match given parameters.");
				}
			}
			if (sb.charAt(0) != '>') {
				throw new IllegalArgumentException("Tuple arity does not match given parameters.");
			}
			// remove '>'
			sb.deleteCharAt(0);

			returnType = new TupleTypeInfo(clazz, types);
		}
		// basic types of classes
		else if (basicTypeMatcher.find()) {
			String className = basicTypeMatcher.group(1);
			sb.delete(0, className.length());
			Class<?> clazz = null;
			// check if fully qualified
			if (className.startsWith("java.lang")) {
				clazz = Class.forName(className);
			} else {
				clazz = Class.forName("java.lang." + className);
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// basic type of primitives
		else if (basicType2Matcher.find()) {
			String className = basicType2Matcher.group(1);
			sb.delete(0, className.length());

			Class<?> clazz = null;
			if (className.equals("int")) {
				clazz = Integer.class;
			} else if (className.equals("byte")) {
				clazz = Byte.class;
			} else if (className.equals("short")) {
				clazz = Short.class;
			} else if (className.equals("char")) {
				clazz = Character.class;
			} else if (className.equals("double")) {
				clazz = Double.class;
			} else if (className.equals("float")) {
				clazz = Float.class;
			} else if (className.equals("long")) {
				clazz = Long.class;
			} else if (className.equals("boolean")) {
				clazz = Boolean.class;
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// values
		else if (valueTypeMatcher.find()) {
			String className = valueTypeMatcher.group(1);
			sb.delete(0, className.length() + 5);

			Class<?> clazz = null;
			// check if fully qualified
			if (className.startsWith(VALUE_PACKAGE)) {
				clazz = Class.forName(className + "Value");
			} else {
				clazz = Class.forName(VALUE_PACKAGE + "." + className + "Value");
			}
			returnType = ValueTypeInfo.getValueTypeInfo((Class<Value>) clazz);
		}
		// array of classes
		else if (basicArrayTypeMatcher.find()) {
			String className = basicArrayTypeMatcher.group(1);
			sb.delete(0, className.length() + 2);

			Class<?> clazz = null;
			if (className.startsWith("java.lang")) {
				clazz = Class.forName("[L" + className + ";");
			} else {
				clazz = Class.forName("[Ljava.lang." + className + ";");
			}
			returnType = BasicArrayTypeInfo.getInfoFor(clazz);
		}
		// array of primitives
		else if (basicArrayType2Matcher.find()) {
			String className = basicArrayType2Matcher.group(1);
			sb.delete(0, className.length() + 2);

			Class<?> clazz = null;
			if (className.equals("int")) {
				clazz = Integer[].class;
			} else if (className.equals("byte")) {
				clazz = Byte[].class;
			} else if (className.equals("short")) {
				clazz = Short[].class;
			} else if (className.equals("char")) {
				clazz = Character[].class;
			} else if (className.equals("double")) {
				clazz = Double[].class;
			} else if (className.equals("float")) {
				clazz = Float[].class;
			} else if (className.equals("long")) {
				clazz = Long[].class;
			} else if (className.equals("boolean")) {
				clazz = Boolean[].class;
			}
			returnType = BasicArrayTypeInfo.getInfoFor(clazz);
		}
		// custom objects
		else if (customObjectMatcher.find()) {
			String fullyQualifiedName = customObjectMatcher.group(1);
			sb.delete(0, fullyQualifiedName.length());

			if (fullyQualifiedName.contains("<")) {
				throw new IllegalArgumentException("Parameterized custom classes are not supported by parser.");
			}

			// custom object array
			if (fullyQualifiedName.endsWith("[]")) {
				fullyQualifiedName = fullyQualifiedName.substring(0, fullyQualifiedName.length() - 2);
				try {
					Class<?> clazz = Class.forName("[L" + fullyQualifiedName + ";");
					returnType = ObjectArrayTypeInfo.getInfoFor(clazz);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Class '" + fullyQualifiedName + "' could not be found for use as object array.");
				}
			} else {
				try {
					Class<?> clazz = Class.forName(fullyQualifiedName);
					returnType = new GenericTypeInfo(clazz);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Class '" + fullyQualifiedName + "' could not be found for use as custom object.");
				}
			}
		}

		if (returnType == null) {
			throw new IllegalArgumentException("Error at '" + infoString + "'");
		} else {
			// remove possible ','
			if (sb.length() > 0 && sb.charAt(0) == ',') {
				sb.deleteCharAt(0);
			}
			return returnType;
		}
	}
}
