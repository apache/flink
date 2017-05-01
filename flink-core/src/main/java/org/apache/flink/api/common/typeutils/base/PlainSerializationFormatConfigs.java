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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;

/**
 * Simple {@link TypeSerializerConfigSnapshot}s for primitive type serializers.
 */
@Internal
public class PlainSerializationFormatConfigs {

	// --------------------------------------------------------------------------------------------
	//  Singleton instances for each kind of serialization format configuration
	// --------------------------------------------------------------------------------------------

	public static final BooleanSerializationFormatConfig BOOLEAN = new BooleanSerializationFormatConfig();
	public static final ByteSerializationFormatConfig BYTE = new ByteSerializationFormatConfig();
	public static final CharSerializationFormatConfig CHAR = new CharSerializationFormatConfig();
	public static final DoubleSerializationFormatConfig DOUBLE = new DoubleSerializationFormatConfig();
	public static final FloatSerializationFormatConfig FLOAT = new FloatSerializationFormatConfig();
	public static final IntSerializationFormatConfig INT = new IntSerializationFormatConfig();
	public static final LongSerializationFormatConfig LONG = new LongSerializationFormatConfig();
	public static final ShortSerializationFormatConfig SHORT = new ShortSerializationFormatConfig();
	public static final StringSerializationFormatConfig STRING = new StringSerializationFormatConfig();
	public static final NullSerializationFormatConfig NULL = new NullSerializationFormatConfig();
	public static final VoidSerializationFormatConfig VOID = new VoidSerializationFormatConfig();

	public static final BooleanArraySerializationFormatConfig BOOLEAN_ARRAY = new BooleanArraySerializationFormatConfig();
	public static final ByteArraySerializationFormatConfig BYTE_ARRAY = new ByteArraySerializationFormatConfig();
	public static final CharArraySerializationFormatConfig CHAR_ARRAY = new CharArraySerializationFormatConfig();
	public static final DoubleArraySerializationFormatConfig DOUBLE_ARRAY = new DoubleArraySerializationFormatConfig();
	public static final FloatArraySerializationFormatConfig FLOAT_ARRAY = new FloatArraySerializationFormatConfig();
	public static final IntArraySerializationFormatConfig INT_ARRAY = new IntArraySerializationFormatConfig();
	public static final LongArraySerializationFormatConfig LONG_ARRAY = new LongArraySerializationFormatConfig();
	public static final ShortArraySerializationFormatConfig SHORT_ARRAY = new ShortArraySerializationFormatConfig();
	public static final StringArraySerializationFormatConfig STRING_ARRAY = new StringArraySerializationFormatConfig();
	public static final NullArraySerializationFormatConfig NULL_ARRAY = new NullArraySerializationFormatConfig();

	// --------------------------------------------------------------------------------------------
	//  Serializer configuration classes for primitive type serializers
	// --------------------------------------------------------------------------------------------

	public static final class BooleanSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class ByteSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class CharSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class DoubleSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class FloatSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class IntSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class LongSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class ShortSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class StringSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class NullSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class VoidSerializationFormatConfig extends ParameterlessTypeSerializerConfig {}

	// --------------------------------------------------------------------------------------------
	//  Serializer configuration classes for primitive type array serializers
	// --------------------------------------------------------------------------------------------

	public static final class BooleanArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class ByteArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class CharArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class DoubleArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class FloatArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class IntArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class LongArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class ShortArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class StringArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
	public static final class NullArraySerializationFormatConfig extends ParameterlessTypeSerializerConfig {}
}
