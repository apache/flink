/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.invokable.operator.ProjectInvokable;

public class StreamProjection<IN> {

	private DataStream<IN> dataStream;
	private int[] fieldIndexes;
	private TypeInformation<IN> inTypeInfo;

	protected StreamProjection(DataStream<IN> dataStream, int[] fieldIndexes) {
		this.dataStream = dataStream;
		this.fieldIndexes = fieldIndexes;
		this.inTypeInfo = dataStream.getType();
		if (!inTypeInfo.isTupleType()) {
			throw new RuntimeException("Only Tuple DataStreams can be projected");
		}
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0> SingleOutputStreamOperator<Tuple1<T0>, ?> types(Class<T0> type0) {
		Class<?>[] types = { type0 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple1<T0>> outType = (TypeInformation<Tuple1<T0>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType, new ProjectInvokable<IN, Tuple1<T0>>(
				fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1> SingleOutputStreamOperator<Tuple2<T0, T1>, ?> types(Class<T0> type0,
			Class<T1> type1) {
		Class<?>[] types = { type0, type1 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple2<T0, T1>> outType = (TypeInformation<Tuple2<T0, T1>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple2<T0, T1>>(fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2> SingleOutputStreamOperator<Tuple3<T0, T1, T2>, ?> types(Class<T0> type0,
			Class<T1> type1, Class<T2> type2) {
		Class<?>[] types = { type0, type1, type2 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple3<T0, T1, T2>> outType = (TypeInformation<Tuple3<T0, T1, T2>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple3<T0, T1, T2>>(fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3> SingleOutputStreamOperator<Tuple4<T0, T1, T2, T3>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
		Class<?>[] types = { type0, type1, type2, type3 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple4<T0, T1, T2, T3>> outType = (TypeInformation<Tuple4<T0, T1, T2, T3>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple4<T0, T1, T2, T3>>(fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4> SingleOutputStreamOperator<Tuple5<T0, T1, T2, T3, T4>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
		Class<?>[] types = { type0, type1, type2, type3, type4 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}
		@SuppressWarnings("unchecked")
		TypeInformation<Tuple5<T0, T1, T2, T3, T4>> outType = (TypeInformation<Tuple5<T0, T1, T2, T3, T4>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple5<T0, T1, T2, T3, T4>>(fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5> SingleOutputStreamOperator<Tuple6<T0, T1, T2, T3, T4, T5>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple6<T0, T1, T2, T3, T4, T5>> outType = (TypeInformation<Tuple6<T0, T1, T2, T3, T4, T5>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple6<T0, T1, T2, T3, T4, T5>>(fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6> SingleOutputStreamOperator<Tuple7<T0, T1, T2, T3, T4, T5, T6>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple7<T0, T1, T2, T3, T4, T5, T6>> outType = (TypeInformation<Tuple7<T0, T1, T2, T3, T4, T5, T6>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform("projection", outType,
						new ProjectInvokable<IN, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fieldIndexes,
								outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7> SingleOutputStreamOperator<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> outType = (TypeInformation<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fieldIndexes,
						outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8> SingleOutputStreamOperator<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> outType = (TypeInformation<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fieldIndexes,
						outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> SingleOutputStreamOperator<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> outType = (TypeInformation<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(
						fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> SingleOutputStreamOperator<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> outType = (TypeInformation<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream.transform("projection", outType,
				new ProjectInvokable<IN, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(
						fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> SingleOutputStreamOperator<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> outType = (TypeInformation<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> SingleOutputStreamOperator<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> outType = (TypeInformation<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> SingleOutputStreamOperator<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> outType = (TypeInformation<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> SingleOutputStreamOperator<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> outType = (TypeInformation<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> SingleOutputStreamOperator<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> outType = (TypeInformation<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> SingleOutputStreamOperator<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> outType = (TypeInformation<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> SingleOutputStreamOperator<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> outType = (TypeInformation<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> SingleOutputStreamOperator<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> outType = (TypeInformation<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> SingleOutputStreamOperator<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> outType = (TypeInformation<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @param type20
	 *            The class of field '20' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> SingleOutputStreamOperator<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19, Class<T20> type20) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19,
				type20 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> outType = (TypeInformation<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @param type20
	 *            The class of field '20' of the result Tuples.
	 * @param type21
	 *            The class of field '21' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> SingleOutputStreamOperator<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19,
				type20, type21 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> outType = (TypeInformation<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @param type20
	 *            The class of field '20' of the result Tuples.
	 * @param type21
	 *            The class of field '21' of the result Tuples.
	 * @param type22
	 *            The class of field '22' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> SingleOutputStreamOperator<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21,
			Class<T22> type22) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19,
				type20, type21, type22 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> outType = (TypeInformation<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @param type20
	 *            The class of field '20' of the result Tuples.
	 * @param type21
	 *            The class of field '21' of the result Tuples.
	 * @param type22
	 *            The class of field '22' of the result Tuples.
	 * @param type23
	 *            The class of field '23' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> SingleOutputStreamOperator<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21,
			Class<T22> type22, Class<T23> type23) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19,
				type20, type21, type22, type23 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> outType = (TypeInformation<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",
						outType,
						new ProjectInvokable<IN, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(
								fieldIndexes, outType));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected
	 * fields. Requires the classes of the fields of the resulting Tuples.
	 * 
	 * @param type0
	 *            The class of field '0' of the result Tuples.
	 * @param type1
	 *            The class of field '1' of the result Tuples.
	 * @param type2
	 *            The class of field '2' of the result Tuples.
	 * @param type3
	 *            The class of field '3' of the result Tuples.
	 * @param type4
	 *            The class of field '4' of the result Tuples.
	 * @param type5
	 *            The class of field '5' of the result Tuples.
	 * @param type6
	 *            The class of field '6' of the result Tuples.
	 * @param type7
	 *            The class of field '7' of the result Tuples.
	 * @param type8
	 *            The class of field '8' of the result Tuples.
	 * @param type9
	 *            The class of field '9' of the result Tuples.
	 * @param type10
	 *            The class of field '10' of the result Tuples.
	 * @param type11
	 *            The class of field '11' of the result Tuples.
	 * @param type12
	 *            The class of field '12' of the result Tuples.
	 * @param type13
	 *            The class of field '13' of the result Tuples.
	 * @param type14
	 *            The class of field '14' of the result Tuples.
	 * @param type15
	 *            The class of field '15' of the result Tuples.
	 * @param type16
	 *            The class of field '16' of the result Tuples.
	 * @param type17
	 *            The class of field '17' of the result Tuples.
	 * @param type18
	 *            The class of field '18' of the result Tuples.
	 * @param type19
	 *            The class of field '19' of the result Tuples.
	 * @param type20
	 *            The class of field '20' of the result Tuples.
	 * @param type21
	 *            The class of field '21' of the result Tuples.
	 * @param type22
	 *            The class of field '22' of the result Tuples.
	 * @param type23
	 *            The class of field '23' of the result Tuples.
	 * @param type24
	 *            The class of field '24' of the result Tuples.
	 * @return The projected DataStream.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> SingleOutputStreamOperator<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>, ?> types(
			Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4,
			Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9,
			Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13,
			Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17,
			Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21,
			Class<T22> type22, Class<T23> type23, Class<T24> type24) {
		Class<?>[] types = { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
				type10, type11, type12, type13, type14, type15, type16, type17, type18, type19,
				type20, type21, type22, type23, type24 };
		if (types.length != this.fieldIndexes.length) {
			throw new IllegalArgumentException(
					"Numbers of projected fields and types do not match.");
		}

		@SuppressWarnings("unchecked")
		TypeInformation<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> outType = (TypeInformation<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>) extractFieldTypes(
				fieldIndexes, types, inTypeInfo);
		return dataStream
				.transform(
						"projection",

						outType,
						new ProjectInvokable<IN, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(
								fieldIndexes, outType));
	}

	public static TypeInformation<?> extractFieldTypes(int[] fields, Class<?>[] givenTypes,
			TypeInformation<?> inType) {

		TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType;
		TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];

		for (int i = 0; i < fields.length; i++) {

			if (inTupleType.getTypeAt(fields[i]).getTypeClass() != givenTypes[i]) {
				throw new IllegalArgumentException(
						"Given types do not match types of input data set.");
			}

			fieldTypes[i] = inTupleType.getTypeAt(fields[i]);
		}

		return new TupleTypeInfo<Tuple>(fieldTypes);
	}

}
