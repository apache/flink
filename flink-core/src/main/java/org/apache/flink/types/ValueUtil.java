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


package org.apache.flink.types;

/**
 * convert the java.lang type into a value type
 */
public final class ValueUtil {
	public static Value toFlinkValueType(Object java)  {
		if (java == null) {
			return NullValue.getInstance();
		}
		if (java.getClass() == java.lang.Boolean.class) {
			return new BooleanValue(((java.lang.Boolean)java).booleanValue());
		}
		if (java.getClass() == java.lang.Integer.class) {
			return new IntValue(((java.lang.Integer)java).intValue());
		}
		if (java.getClass() == java.lang.Byte.class) {
			return new ByteValue(((java.lang.Byte)java).byteValue());
		}
		if (java.getClass() == java.lang.Character.class) {
			return new CharValue(((java.lang.Character)java).charValue());
		}
		if (java.getClass() == java.lang.Double.class) {
			return new DoubleValue(((java.lang.Double)java).doubleValue());
		}
		if (java.getClass() == java.lang.Float.class) {
			return new FloatValue(((java.lang.Float)java).floatValue());
		}
		if (java.getClass() == java.lang.Long.class) {
			return new LongValue(((java.lang.Long)java).longValue());
		}
		if (java.getClass() == java.lang.Short.class) {
			return new ShortValue(((java.lang.Short)java).shortValue());
		}
		if (java.getClass() == java.lang.String.class) {
			return new StringValue(((java.lang.String)java).toString());
		}
		throw new IllegalArgumentException("unsupported Java value");
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ValueUtil() {
		throw new RuntimeException();
	}
}
