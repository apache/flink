/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.utils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
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
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.util.Preconditions;

public class SiddhiTupleFactory {
	/**
	 * Convert object array to type of Tuple{N} where N is between 0 to 25.
	 */
	public static <T extends Tuple> T newTuple(Object[] row) {
		Preconditions.checkNotNull(row, "Tuple row is null");
		switch (row.length) {
			case 0:
				return setTupleValue(new Tuple0(), row);
			case 1:
				return setTupleValue(new Tuple1(), row);
			case 2:
				return setTupleValue(new Tuple2(), row);
			case 3:
				return setTupleValue(new Tuple3(), row);
			case 4:
				return setTupleValue(new Tuple4(), row);
			case 5:
				return setTupleValue(new Tuple5(), row);
			case 6:
				return setTupleValue(new Tuple6(), row);
			case 7:
				return setTupleValue(new Tuple7(), row);
			case 8:
				return setTupleValue(new Tuple8(), row);
			case 9:
				return setTupleValue(new Tuple9(), row);
			case 10:
				return setTupleValue(new Tuple10(), row);
			case 11:
				return setTupleValue(new Tuple11(), row);
			case 12:
				return setTupleValue(new Tuple12(), row);
			case 13:
				return setTupleValue(new Tuple13(), row);
			case 14:
				return setTupleValue(new Tuple14(), row);
			case 15:
				return setTupleValue(new Tuple15(), row);
			case 16:
				return setTupleValue(new Tuple16(), row);
			case 17:
				return setTupleValue(new Tuple17(), row);
			case 18:
				return setTupleValue(new Tuple18(), row);
			case 19:
				return setTupleValue(new Tuple19(), row);
			case 20:
				return setTupleValue(new Tuple20(), row);
			case 21:
				return setTupleValue(new Tuple21(), row);
			case 22:
				return setTupleValue(new Tuple22(), row);
			case 23:
				return setTupleValue(new Tuple23(), row);
			case 24:
				return setTupleValue(new Tuple24(), row);
			case 25:
				return setTupleValue(new Tuple25(), row);
			default:
				throw new IllegalArgumentException("Too long row: " + row.length + ", unable to convert to Tuple");
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Tuple> T setTupleValue(Tuple tuple, Object[] row) {
		if (row.length != tuple.getArity()) {
			throw new IllegalArgumentException("Row length" + row.length + " is not equal with tuple's arity: " + tuple.getArity());
		}
		for (int i = 0; i < row.length; i++) {
			tuple.setField(row[i], i);
		}
		return (T) tuple;
	}
}
