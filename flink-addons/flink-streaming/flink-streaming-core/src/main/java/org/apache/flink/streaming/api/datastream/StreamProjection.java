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
import org.apache.flink.streaming.api.invokable.operator.ProjectInvokable;
import org.apache.flink.streaming.util.serialization.ProjectTypeWrapper;
import org.apache.flink.streaming.util.serialization.TypeWrapper;

public class StreamProjection<IN> {

	private DataStream<IN> dataStream;
	private int[] fieldIndexes;
	private TypeWrapper<IN> inTypeWrapper;

	protected StreamProjection(DataStream<IN> dataStream, int[] fieldIndexes) {
		this.dataStream = dataStream;
		this.fieldIndexes = fieldIndexes;
		this.inTypeWrapper = dataStream.outTypeWrapper;
		if (!inTypeWrapper.getTypeInfo().isTupleType()) {
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

		TypeWrapper<Tuple1<T0>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple1<T0>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple1<T0>>(fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple2<T0, T1>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple2<T0, T1>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple2<T0, T1>>(fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple3<T0, T1, T2>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple3<T0, T1, T2>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple3<T0, T1, T2>>(fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple4<T0, T1, T2, T3>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple4<T0, T1, T2, T3>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple4<T0, T1, T2, T3>>(fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple5<T0, T1, T2, T3, T4>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple5<T0, T1, T2, T3, T4>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple5<T0, T1, T2, T3, T4>>(fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple6<T0, T1, T2, T3, T4, T5>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple6<T0, T1, T2, T3, T4, T5>>(
				inTypeWrapper, fieldIndexes, types);

		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple6<T0, T1, T2, T3, T4, T5>>(fieldIndexes,
						outTypeWrapper));
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
		TypeWrapper<Tuple7<T0, T1, T2, T3, T4, T5, T6>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fieldIndexes,
						outTypeWrapper));

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
		TypeWrapper<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fieldIndexes,
						outTypeWrapper));

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

		TypeWrapper<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fieldIndexes,
						outTypeWrapper));
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

		TypeWrapper<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(
						fieldIndexes, outTypeWrapper));
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
		TypeWrapper<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream.addFunction("projection", null, inTypeWrapper, outTypeWrapper,
				new ProjectInvokable<IN, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(
						fieldIndexes, outTypeWrapper));

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
		TypeWrapper<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(
								fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(
								fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(
								fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(
								fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(
								fieldIndexes, outTypeWrapper));

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

		TypeWrapper<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(
								fieldIndexes, outTypeWrapper));
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

		TypeWrapper<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> outTypeWrapper = new ProjectTypeWrapper<IN, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(
				inTypeWrapper, fieldIndexes, types);
		return dataStream
				.addFunction(
						"projection",
						null,
						inTypeWrapper,
						outTypeWrapper,
						new ProjectInvokable<IN, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(
								fieldIndexes, outTypeWrapper));
	}

}
