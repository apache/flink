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

package org.apache.flink.api.java.typeutils;

 import org.apache.flink.annotation.Public;
 import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
 import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
 import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
 import org.apache.flink.api.common.typeinfo.TypeInformation;
 import org.apache.flink.types.Value;

 import java.lang.reflect.Field;
 import java.util.ArrayList;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;

/**
 * @deprecated Use {@link org.apache.flink.api.common.typeinfo.Types} instead.
 */
@Deprecated
@Public
public class TypeInfoParser {
	private static final String TUPLE_PACKAGE = "org.apache.flink.api.java.tuple";
	private static final String VALUE_PACKAGE = "org.apache.flink.types";
	private static final String WRITABLE_PACKAGE = "org.apache.hadoop.io";

	private static final Pattern tuplePattern = Pattern.compile("^(" + TUPLE_PACKAGE.replaceAll("\\.", "\\\\.") + "\\.)?((Tuple[1-9][0-9]?)<|(Tuple0))");
	private static final Pattern writablePattern = Pattern.compile("^((" + WRITABLE_PACKAGE.replaceAll("\\.", "\\\\.") + "\\.)?Writable)<([^\\s,>]*)(,|>|$|\\[)");
	private static final Pattern enumPattern = Pattern.compile("^((java\\.lang\\.)?Enum)<([^\\s,>]*)(,|>|$|\\[)");
	private static final Pattern basicTypePattern = Pattern
			.compile("^((java\\.lang\\.)?(String|Integer|Byte|Short|Character|Double|Float|Long|Boolean|Void))(,|>|$|\\[)");
	private static final Pattern basicTypeDatePattern = Pattern.compile("^((java\\.util\\.)?Date)(,|>|$|\\[)");
	private static final Pattern basicTypeBigIntPattern = Pattern.compile("^((java\\.math\\.)?BigInteger)(,|>|$|\\[)");
	private static final Pattern basicTypeBigDecPattern = Pattern.compile("^((java\\.math\\.)?BigDecimal)(,|>|$|\\[)");
	private static final Pattern primitiveTypePattern = Pattern.compile("^(int|byte|short|char|double|float|long|boolean|void)(,|>|$|\\[)");
	private static final Pattern valueTypePattern = Pattern.compile("^((" + VALUE_PACKAGE.replaceAll("\\.", "\\\\.")
			+ "\\.)?(String|Int|Byte|Short|Char|Double|Float|Long|Boolean|List|Map|Null))Value(,|>|$|\\[)");
	private static final Pattern pojoGenericObjectPattern = Pattern.compile("^([^\\s,<>\\[]+)(<)?");
	private static final Pattern fieldPattern = Pattern.compile("^([^\\s,<>\\[]+)=");

	/**
	 * Generates an instance of <code>TypeInformation</code> by parsing a type
	 * information string. A type information string can contain the following
	 * types:
	 *
	 * <ul>
	 * <li>Basic types such as <code>Integer</code>, <code>String</code>, etc.
	 * <li>Basic type arrays such as <code>Integer[]</code>,
	 * <code>String[]</code>, etc.
	 * <li>Tuple types such as <code>Tuple1&lt;TYPE0&gt;</code>,
	 * <code>Tuple2&lt;TYPE0, TYPE1&gt;</code>, etc.</li>
	 * <li>Pojo types such as <code>org.my.MyPojo&lt;myFieldName=TYPE0,myFieldName2=TYPE1&gt;</code>, etc.</li>
	 * <li>Generic types such as <code>java.lang.Class</code>, etc.
	 * <li>Custom type arrays such as <code>org.my.CustomClass[]</code>,
	 * <code>org.my.CustomClass$StaticInnerClass[]</code>, etc.
	 * <li>Value types such as <code>DoubleValue</code>,
	 * <code>StringValue</code>, <code>IntegerValue</code>, etc.</li>
	 * <li>Tuple array types such as <code>Tuple2&lt;TYPE0,TYPE1&gt;[], etc.</code></li>
	 * <li>Writable types such as <code>Writable&lt;org.my.CustomWritable&gt;</code></li>
	 * <li>Enum types such as <code>Enum&lt;org.my.CustomEnum&gt;</code></li>
	 * </ul>
	 *
	 * Example:
	 * <code>"Tuple2&lt;String,Tuple2&lt;Integer,org.my.MyJob$Pojo&lt;word=String&gt;&gt;&gt;"</code>
	 *
	 * @param infoString
	 *            type information string to be parsed
	 * @return <code>TypeInformation</code> representation of the string
	 */
	@SuppressWarnings("unchecked")
	public static <X> TypeInformation<X> parse(String infoString) {
		try {
			if (infoString == null) {
				throw new IllegalArgumentException("String is null.");
			}
			String clearedString = infoString.replaceAll("\\s", "");
			if (clearedString.length() == 0) {
				throw new IllegalArgumentException("String must not be empty.");
			}
			StringBuilder sb = new StringBuilder(clearedString);
			TypeInformation<X> ti = (TypeInformation<X>) parse(sb);
			if (sb.length() > 0) {
				throw new IllegalArgumentException("String could not be parsed completely.");
			}
			return ti;
		} catch (Exception e) {
			throw new IllegalArgumentException("String could not be parsed: " + e.getMessage(), e);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static TypeInformation<?> parse(StringBuilder sb) throws ClassNotFoundException {
		String infoString = sb.toString();
		final Matcher tupleMatcher = tuplePattern.matcher(infoString);

		final Matcher writableMatcher = writablePattern.matcher(infoString);
		final Matcher enumMatcher = enumPattern.matcher(infoString);

		final Matcher basicTypeMatcher = basicTypePattern.matcher(infoString);
		final Matcher basicTypeDateMatcher = basicTypeDatePattern.matcher(infoString);
		final Matcher basicTypeBigIntMatcher = basicTypeBigIntPattern.matcher(infoString);
		final Matcher basicTypeBigDecMatcher = basicTypeBigDecPattern.matcher(infoString);

		final Matcher primitiveTypeMatcher = primitiveTypePattern.matcher(infoString);

		final Matcher valueTypeMatcher = valueTypePattern.matcher(infoString);

		final Matcher pojoGenericMatcher = pojoGenericObjectPattern.matcher(infoString);

		if (infoString.length() == 0) {
			return null;
		}

		TypeInformation<?> returnType = null;
		boolean isPrimitiveType = false;

		// tuples
		if (tupleMatcher.find()) {
			boolean isGenericTuple = true;
			String className = tupleMatcher.group(3);
			if(className == null) { // matched Tuple0
				isGenericTuple = false;
				className = tupleMatcher.group(2);
				sb.delete(0, className.length());
			} else {
				sb.delete(0, className.length() + 1); // +1 for "<"
			}

			if (infoString.startsWith(TUPLE_PACKAGE)) {
				sb.delete(0, TUPLE_PACKAGE.length() + 1); // +1 for trailing "."
			}

			int arity = Integer.parseInt(className.replaceAll("\\D", ""));
			Class<?> clazz = loadClass(TUPLE_PACKAGE + "." + className);

			TypeInformation<?>[] types = new TypeInformation<?>[arity];
			for (int i = 0; i < arity; i++) {
				types[i] = parse(sb);
				if (types[i] == null) {
					throw new IllegalArgumentException("Tuple arity does not match given parameters.");
				}
			}
			if (isGenericTuple) {
				if(sb.charAt(0) != '>') {
					throw new IllegalArgumentException("Tuple arity does not match given parameters.");
				}
				// remove '>'
				sb.deleteCharAt(0);
			}
			returnType = new TupleTypeInfo(clazz, types);
		}
		// writable types
		else if (writableMatcher.find()) {
			String className = writableMatcher.group(1);
			String fullyQualifiedName = writableMatcher.group(3);
			sb.delete(0, className.length() + 1 + fullyQualifiedName.length() + 1);
			Class<?> clazz = loadClass(fullyQualifiedName);
			returnType = TypeExtractor.createHadoopWritableTypeInfo(clazz);
		}
		// enum types
		else if (enumMatcher.find()) {
			String className = enumMatcher.group(1);
			String fullyQualifiedName = enumMatcher.group(3);
			sb.delete(0, className.length() + 1 + fullyQualifiedName.length() + 1);
			Class<?> clazz = loadClass(fullyQualifiedName);
			returnType = new EnumTypeInfo(clazz);
		}
		// basic types
		else if (basicTypeMatcher.find()) {
			String className = basicTypeMatcher.group(1);
			sb.delete(0, className.length());
			Class<?> clazz;
			// check if fully qualified
			if (className.startsWith("java.lang")) {
				clazz = loadClass(className);
			} else {
				clazz = loadClass("java.lang." + className);
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// special basic type "Date"
		else if (basicTypeDateMatcher.find()) {
			String className = basicTypeDateMatcher.group(1);
			sb.delete(0, className.length());
			Class<?> clazz;
			// check if fully qualified
			if (className.startsWith("java.util")) {
				clazz = loadClass(className);
			} else {
				clazz = loadClass("java.util." + className);
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// special basic type "BigInteger"
		else if (basicTypeBigIntMatcher.find()) {
			String className = basicTypeBigIntMatcher.group(1);
			sb.delete(0, className.length());
			Class<?> clazz;
			// check if fully qualified
			if (className.startsWith("java.math")) {
				clazz = loadClass(className);
			} else {
				clazz = loadClass("java.math." + className);
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// special basic type "BigDecimal"
		else if (basicTypeBigDecMatcher.find()) {
			String className = basicTypeBigDecMatcher.group(1);
			sb.delete(0, className.length());
			Class<?> clazz;
			// check if fully qualified
			if (className.startsWith("java.math")) {
				clazz = loadClass(className);
			} else {
				clazz = loadClass("java.math." + className);
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
		}
		// primitive types
		else if (primitiveTypeMatcher.find()) {
			String keyword = primitiveTypeMatcher.group(1);
			sb.delete(0, keyword.length());

			Class<?> clazz = null;
			if (keyword.equals("int")) {
				clazz = int.class;
			} else if (keyword.equals("byte")) {
				clazz = byte.class;
			} else if (keyword.equals("short")) {
				clazz = short.class;
			} else if (keyword.equals("char")) {
				clazz = char.class;
			} else if (keyword.equals("double")) {
				clazz = double.class;
			} else if (keyword.equals("float")) {
				clazz = float.class;
			} else if (keyword.equals("long")) {
				clazz = long.class;
			} else if (keyword.equals("boolean")) {
				clazz = boolean.class;
			} else if (keyword.equals("void")) {
				clazz = void.class;
			}
			returnType = BasicTypeInfo.getInfoFor(clazz);
			isPrimitiveType = true;
		}
		// values
		else if (valueTypeMatcher.find()) {
			String className = valueTypeMatcher.group(1);
			sb.delete(0, className.length() + 5);

			Class<?> clazz;
			// check if fully qualified
			if (className.startsWith(VALUE_PACKAGE)) {
				clazz = loadClass(className + "Value");
			} else {
				clazz = loadClass(VALUE_PACKAGE + "." + className + "Value");
			}
			returnType = ValueTypeInfo.getValueTypeInfo((Class<Value>) clazz);
		}
		// pojo objects or generic types
		else if (pojoGenericMatcher.find()) {
			String fullyQualifiedName = pojoGenericMatcher.group(1);
			sb.delete(0, fullyQualifiedName.length());

			boolean isPojo = pojoGenericMatcher.group(2) != null;

			// pojo
			if (isPojo) {
				sb.deleteCharAt(0);
				Class<?> clazz = loadClass(fullyQualifiedName);

				ArrayList<PojoField> fields = new ArrayList<PojoField>();
				while (sb.charAt(0) != '>') {
					final Matcher fieldMatcher = fieldPattern.matcher(sb);
					if (!fieldMatcher.find()) {
						throw new IllegalArgumentException("Field name missing.");
					}
					String fieldName = fieldMatcher.group(1);
					sb.delete(0, fieldName.length() + 1);

					Field field = TypeExtractor.getDeclaredField(clazz, fieldName);
					if (field == null) {
						throw new IllegalArgumentException("Field '" + fieldName + "'could not be accessed.");
					}
					fields.add(new PojoField(field, parse(sb)));
				}
				sb.deleteCharAt(0); // remove '>'
				returnType = new PojoTypeInfo(clazz, fields);
			}
			// generic type
			else {
				returnType = new GenericTypeInfo(loadClass(fullyQualifiedName));
			}
		}

		if (returnType == null) {
			throw new IllegalArgumentException("Error at '" + infoString + "'");
		}

		// arrays
		int arrayDimensionCount = 0;
		while (sb.length() > 1 && sb.charAt(0) == '[' && sb.charAt(1) == ']') {
			arrayDimensionCount++;
			sb.delete(0, 2);
		}

		if (sb.length() > 0 && sb.charAt(0) == '[') {
			throw new IllegalArgumentException("Closing square bracket missing.");
		}
		
		// construct multidimension array
		if (arrayDimensionCount > 0) {
			TypeInformation<?> arrayInfo = null;
			
			// first dimension
			// primitive array
			if (isPrimitiveType) {
				if (returnType == BasicTypeInfo.INT_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.BYTE_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.SHORT_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.CHAR_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.DOUBLE_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.FLOAT_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.LONG_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
					arrayInfo = PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.VOID_TYPE_INFO) {
					throw new IllegalArgumentException("Can not create an array of void.");
				}
			}
			// basic array
			else if (returnType instanceof BasicTypeInfo
					&& returnType != BasicTypeInfo.DATE_TYPE_INFO) {
				if (returnType == BasicTypeInfo.INT_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.BYTE_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.SHORT_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.CHAR_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.DOUBLE_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.FLOAT_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.LONG_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.STRING_TYPE_INFO) {
					arrayInfo = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
				} else if (returnType == BasicTypeInfo.VOID_TYPE_INFO) {
					throw new IllegalArgumentException("Can not create an array of void.");
				}
			}
			// object array
			else {
				arrayInfo = ObjectArrayTypeInfo.getInfoFor(loadClass("[L" + returnType.getTypeClass().getName() + ";"),
						returnType);
			}

			// further dimensions
			if (arrayDimensionCount > 1) {
				String arrayPrefix = "[";
				for (int i = 1; i < arrayDimensionCount; i++) {
					arrayPrefix += "[";
					arrayInfo =  ObjectArrayTypeInfo.getInfoFor(loadClass(arrayPrefix + "L" +
							returnType.getTypeClass().getName() + ";"), arrayInfo);
				}
			}
			returnType = arrayInfo;
		}

		// remove possible ','
		if (sb.length() > 0 && sb.charAt(0) == ',') {
			sb.deleteCharAt(0);
		}

		// check if end 
		return returnType;
	}

	private static Class<?> loadClass(String fullyQualifiedName) {
		try {
			return Class.forName(fullyQualifiedName);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Class '" + fullyQualifiedName
					+ "' could not be found. Please note that inner classes must be declared static.");
		}
	}

}
