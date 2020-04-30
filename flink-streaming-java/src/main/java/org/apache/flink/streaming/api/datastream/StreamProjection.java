/*
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

import org.apache.flink.annotation.PublicEvolving;
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
import org.apache.flink.streaming.api.operators.StreamProject;
import org.apache.flink.util.Preconditions;

/**
 * The result of {@link DataStream#project(int...)}. This can be used to add more fields to the
 * projection.
 */
@PublicEvolving
public class StreamProjection<IN> {

	private DataStream<IN> dataStream;
	private int[] fieldIndexes;

	protected StreamProjection(DataStream<IN> dataStream, int[] fieldIndexes) {
		if (!dataStream.getType().isTupleType()) {
			throw new RuntimeException("Only Tuple DataStreams can be projected");
		}
		if (fieldIndexes.length == 0) {
			throw new IllegalArgumentException("project() needs to select at least one (1) field.");
		} else if (fieldIndexes.length > Tuple.MAX_ARITY - 1) {
			throw new IllegalArgumentException(
					"project() may select only up to (" + (Tuple.MAX_ARITY - 1) + ") fields.");
		}

		int maxFieldIndex = (dataStream.getType()).getArity();
		for (int i = 0; i < fieldIndexes.length; i++) {
			Preconditions.checkElementIndex(fieldIndexes[i], maxFieldIndex);
		}

		this.dataStream = dataStream;
		this.fieldIndexes = fieldIndexes;
	}

	/**
	 * Chooses a projectTupleX according to the length of
	 * {@link org.apache.flink.streaming.api.datastream.StreamProjection#fieldIndexes}.
	 *
	 * @return The projected DataStream.
	 * @see org.apache.flink.api.java.operators.ProjectOperator.Projection
	 */
	@SuppressWarnings("unchecked")
	public <OUT extends Tuple> SingleOutputStreamOperator<OUT> projectTupleX() {
		SingleOutputStreamOperator<OUT> projOperator = null;

		switch (fieldIndexes.length) {
			case 1: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple1(); break;
			case 2: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple2(); break;
			case 3: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple3(); break;
			case 4: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple4(); break;
			case 5: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple5(); break;
			case 6: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple6(); break;
			case 7: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple7(); break;
			case 8: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple8(); break;
			case 9: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple9(); break;
			case 10: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple10(); break;
			case 11: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple11(); break;
			case 12: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple12(); break;
			case 13: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple13(); break;
			case 14: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple14(); break;
			case 15: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple15(); break;
			case 16: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple16(); break;
			case 17: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple17(); break;
			case 18: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple18(); break;
			case 19: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple19(); break;
			case 20: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple20(); break;
			case 21: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple21(); break;
			case 22: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple22(); break;
			case 23: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple23(); break;
			case 24: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple24(); break;
			case 25: projOperator = (SingleOutputStreamOperator<OUT>) projectTuple25(); break;
			default:
				throw new IllegalStateException("Excessive arity in tuple.");
		}

		return projOperator;
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0> SingleOutputStreamOperator<Tuple1<T0>> projectTuple1() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple1<T0>>(
				fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1> SingleOutputStreamOperator<Tuple2<T0, T1>> projectTuple2() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple2<T0, T1>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2> SingleOutputStreamOperator<Tuple3<T0, T1, T2>> projectTuple3() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple3<T0, T1, T2>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3> SingleOutputStreamOperator<Tuple4<T0, T1, T2, T3>> projectTuple4() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple4<T0, T1, T2, T3>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4> SingleOutputStreamOperator<Tuple5<T0, T1, T2, T3, T4>> projectTuple5() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple5<T0, T1, T2, T3, T4>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5> SingleOutputStreamOperator<Tuple6<T0, T1, T2, T3, T4, T5>> projectTuple6() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple6<T0, T1, T2, T3, T4, T5>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6> SingleOutputStreamOperator<Tuple7<T0, T1, T2, T3, T4, T5, T6>> projectTuple7() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7> SingleOutputStreamOperator<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> projectTuple8() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8> SingleOutputStreamOperator<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> projectTuple9() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> SingleOutputStreamOperator<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> projectTuple10() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> SingleOutputStreamOperator<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> projectTuple11() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> SingleOutputStreamOperator<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> projectTuple12() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> SingleOutputStreamOperator<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> projectTuple13() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> SingleOutputStreamOperator<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> projectTuple14() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> SingleOutputStreamOperator<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> projectTuple15() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> SingleOutputStreamOperator<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> projectTuple16() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> SingleOutputStreamOperator<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> projectTuple17() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> SingleOutputStreamOperator<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> projectTuple18() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> SingleOutputStreamOperator<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> projectTuple19() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> SingleOutputStreamOperator<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> projectTuple20() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> SingleOutputStreamOperator<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> projectTuple21() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> SingleOutputStreamOperator<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> projectTuple22() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> SingleOutputStreamOperator<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> projectTuple23() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> tType = new TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> SingleOutputStreamOperator<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> projectTuple24() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> tType = new TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	/**
	 * Projects a {@link Tuple} {@link DataStream} to the previously selected fields.
	 *
	 * @return The projected DataStream.
	 * @see Tuple
	 * @see DataStream
	 */
	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> SingleOutputStreamOperator<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> projectTuple25() {
		TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, dataStream.getType());
		TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> tType = new TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fTypes);

		return dataStream.transform("Projection", tType, new StreamProject<IN, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fieldIndexes, tType.createSerializer(dataStream.getExecutionConfig())));
	}

	public static TypeInformation<?>[] extractFieldTypes(int[] fields, TypeInformation<?> inType) {

		TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType;
		TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];

		for (int i = 0; i < fields.length; i++) {
			fieldTypes[i] = inTupleType.getTypeAt(fields[i]);
		}

		return fieldTypes;
	}

}
