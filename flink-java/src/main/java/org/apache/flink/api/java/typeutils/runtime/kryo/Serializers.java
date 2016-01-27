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
package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaIntervalSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * Class containing utilities for the serializers of the Flink Runtime.
 *
 * Most of the serializers are automatically added to the system.
 *
 * Note that users can also implement the {@link com.esotericsoftware.kryo.KryoSerializable} interface
 * to provide custom serialization for their classes.
 * Also, there is a Java Annotation for adding a default serializer (@DefaultSerializer) to classes.
 */
public class Serializers {
	/**
	 * NOTE: This method is not a public Flink API.
	 *
	 * This method walks the entire hierarchy of the given type and registers all types it encounters
	 * to Kryo.
	 * It also watches for types which need special serializers.
	 */
	private static Set<Class<?>> alreadySeen = new HashSet<Class<?>>();

	public static void recursivelyRegisterType(Class<?> type, ExecutionConfig config) {
		alreadySeen.add(type);
		if(type.isPrimitive()) {
			return;
		}
		config.registerKryoType(type);
		addSerializerForType(config, type);

		Field[] fields = type.getDeclaredFields();
		for(Field field : fields) {
			if(Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
				continue;
			}
			Type fieldType = field.getGenericType();
			if(fieldType instanceof ParameterizedType) { // field has generics
				ParameterizedType parameterizedFieldType = (ParameterizedType) fieldType;
				for(Type t: parameterizedFieldType.getActualTypeArguments()) {
					if(TypeExtractor.isClassType(t) ) {
						Class clazz = TypeExtractor.typeToClass(t);
						if(!alreadySeen.contains(clazz)) {
							recursivelyRegisterType(TypeExtractor.typeToClass(t), config);
						}
					}
				}
			}
			Class<?> clazz = field.getType();
			if(!alreadySeen.contains(clazz)) {
				recursivelyRegisterType(clazz, config);
			}
		}
	}

	public static void addSerializerForType(ExecutionConfig reg, Class<?> type) {
		if(GenericData.Record.class.isAssignableFrom(type)) {
			registerGenericAvro(reg);
		}
		if(SpecificRecordBase.class.isAssignableFrom(type)) {
			registerSpecificAvro(reg, (Class<? extends SpecificRecordBase>) type);
		}
		if(DateTime.class.isAssignableFrom(type) || Interval.class.isAssignableFrom(type)) {
			registerJodaTime(reg);
		}
	}

	/**
	 * Register these serializers for using Avro's {@link GenericData.Record} and classes
	 * implementing {@link org.apache.avro.specific.SpecificRecordBase}
	 */
	public static void registerGenericAvro(ExecutionConfig reg) {
		// Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
		// because Kryo is not able to serialize them properly, we use this serializer for them
		reg.registerTypeWithKryoSerializer(GenericData.Array.class, SpecificInstanceCollectionSerializerForArrayList.class);
		// We register this serializer for users who want to use untyped Avro records (GenericData.Record).
		// Kryo is able to serialize everything in there, except for the Schema.
		// This serializer is very slow, but using the GenericData.Records of Kryo is in general a bad idea.
		// we add the serializer as a default serializer because Avro is using a private sub-type at runtime.
		reg.addDefaultKryoSerializer(Schema.class, AvroSchemaSerializer.class);
	}


	public static void registerSpecificAvro(ExecutionConfig reg, Class<? extends SpecificRecordBase> avroType) {
		registerGenericAvro(reg);
		// This rule only applies if users explicitly use the GenericTypeInformation for the avro types
		// usually, we are able to handle Avro POJOs with the POJO serializer.
		// (However only if the GenericData.Array type is registered!)

	//	ClassTag<SpecificRecordBase> tag = scala.reflect.ClassTag$.MODULE$.apply(avroType);
	//	reg.registerTypeWithKryoSerializer(avroType, com.twitter.chill.avro.AvroSerializer.SpecificRecordSerializer(tag));
	}


	/**
	 * Currently, the following classes of JodaTime are supported:
	 * 	- DateTime
	 * 	- Interval
	 *
	 * 	The following chronologies are supported: (see {@link de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer})
	 * <ul>
	 * <li>{@link org.joda.time.chrono.ISOChronology}</li>
	 * <li>{@link org.joda.time.chrono.CopticChronology}</li>
	 * <li>{@link org.joda.time.chrono.EthiopicChronology}</li>
	 * <li>{@link org.joda.time.chrono.GregorianChronology}</li>
	 * <li>{@link org.joda.time.chrono.JulianChronology}</li>
	 * <li>{@link org.joda.time.chrono.IslamicChronology}</li>
	 * <li>{@link org.joda.time.chrono.BuddhistChronology}</li>
	 * <li>{@link org.joda.time.chrono.GJChronology}</li>
	 * </ul>
	 */
	public static void registerJodaTime(ExecutionConfig reg) {
		reg.registerTypeWithKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);
		reg.registerTypeWithKryoSerializer(Interval.class, JodaIntervalSerializer.class);
	}

	/**
	 * Register less frequently used serializers
	 */
	public static void registerJavaUtils(ExecutionConfig reg) {
		// BitSet, Regex is already present through twitter-chill.
	}


	// --------------------------------------------------------------------------------------------
	// Custom Serializers
	// --------------------------------------------------------------------------------------------

	public static class SpecificInstanceCollectionSerializerForArrayList extends SpecificInstanceCollectionSerializer<ArrayList> {
		public SpecificInstanceCollectionSerializerForArrayList() {
			super(ArrayList.class);
		}
	}
	/**
	 * Special serializer for Java collections enforcing certain instance types.
	 * Avro is serializing collections with an "GenericData.Array" type. Kryo is not able to handle
	 * this type, so we use ArrayLists.
	 */
	public static class SpecificInstanceCollectionSerializer<T extends Collection> extends CollectionSerializer implements Serializable {
		private Class<T> type;
		public SpecificInstanceCollectionSerializer(Class<T> type) {
			this.type = type;
		}

		@Override
		protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
			return kryo.newInstance(this.type);
		}

		@Override
		protected Collection createCopy(Kryo kryo, Collection original) {
			return kryo.newInstance(this.type);
		}
	}

	/**
	 * Slow serialization approach for Avro schemas.
	 * This is only used with {{@link org.apache.avro.generic.GenericData.Record}} types.
	 * Having this serializer, we are able to handle avro Records.
	 */
	public static class AvroSchemaSerializer extends Serializer<Schema> implements Serializable {
		@Override
		public void write(Kryo kryo, Output output, Schema object) {
			String schemaAsString = object.toString(false);
			output.writeString(schemaAsString);
		}

		@Override
		public Schema read(Kryo kryo, Input input, Class<Schema> type) {
			String schemaAsString = input.readString();
			// the parser seems to be stateful, to we need a new one for every type.
			Schema.Parser sParser = new Schema.Parser();
			return sParser.parse(schemaAsString);
		}
	}
}
