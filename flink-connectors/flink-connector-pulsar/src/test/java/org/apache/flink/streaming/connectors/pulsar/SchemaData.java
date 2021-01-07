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

package org.apache.flink.streaming.connectors.pulsar;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Data for various test cases.
 */
public class SchemaData {

	public static final List<Boolean> BOOLEAN_LIST = Arrays.asList(true, false, true, true, false);
	public static final List<Integer> INTEGER_LIST = Arrays.asList(1, 2, 3, 4, 5);
	public static final List<byte[]> BYTES_LIST = INTEGER_LIST
		.stream()
		.map(i -> i.toString().getBytes())
		.collect(Collectors.toList());
	public static final List<Byte> INT_8_LIST = INTEGER_LIST
		.stream()
		.map(Integer::byteValue)
		.collect(Collectors.toList());
	public static final List<Short> INT_16_LIST = INTEGER_LIST
		.stream()
		.map(Integer::shortValue)
		.collect(Collectors.toList());
	public static final List<Long> INT_64_LIST = INTEGER_LIST
		.stream()
		.map(Integer::longValue)
		.collect(Collectors.toList());
	public static final List<Double> DOUBLE_LIST = INTEGER_LIST
		.stream()
		.map(Integer::doubleValue)
		.collect(Collectors.toList());
	public static final List<Float> FLOAT_LIST = INTEGER_LIST
		.stream()
		.map(Integer::floatValue)
		.collect(Collectors.toList());
	public static final List<String> STRING_LIST = INTEGER_LIST
		.stream()
		.map(Objects::toString)
		.collect(Collectors.toList());
	public static List<LocalDate> localDateList;
	public static List<LocalDateTime> localDateTimeList;
	public static List<FA> faList;
	public static List<Foo> fooList;
	public static List<FL> flList;
	public static List<FM> fmList;

	static {
		localDateList = INTEGER_LIST.stream()
			.map(i -> LocalDate.of(2019, 1, i))
			.collect(Collectors.toList());

		localDateTimeList = INTEGER_LIST.stream()
			.map(i -> LocalDateTime.of(2019, 1, i, 20, 35, 40))
			.collect(Collectors.toList());

		fooList = Arrays.asList(
			new Foo(1, 1.0f, new Bar(true, "a")),
			new Foo(2, 2.0f, new Bar(false, "b")),
			new Foo(3, 0, null),
			new Foo(0, 0, null));

		flList = Arrays.asList(
			new FL(Arrays.asList(
				new Bar(true, "a"))),
			new FL(Arrays.asList(
				new Bar(false, "b"))),
			new FL(Arrays.asList(
				new Bar(true, "b")))
		);

		faList = Arrays.asList(
			new FA(new Bar[]{new Bar(true, "a")}),
			new FA(new Bar[]{new Bar(false, "b")}),
			new FA(new Bar[]{new Bar(true, "b")}));

		fmList = Arrays.asList(
			new FM(Collections.singletonMap("a", new Bar(true, "a"))),
			new FM(Collections.singletonMap("b", new Bar(false, "b"))),
			new FM(Collections.singletonMap("c", new Bar(true, "a")))
		);
	}

	/**
	 * Foo type.
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class Foo {
		public int i;
		public float f;
		public Bar bar;

		@Override
		public String toString() {
			return "" + i + "," + f + "," + (bar == null ? "null" : bar.toString());
		}
	}

	/**
	 * Bar type.
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class Bar {
		public boolean b;
		public String s;

		@Override
		public String toString() {
			return "" + b + "," + s;
		}
	}

	/**
	 * FL type.
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class FL {
		public List<Bar> l;

		@Override
		public String toString() {
			if (l == null) {
				return "null";
			} else {
				StringBuilder sb = new StringBuilder();

				for (int i = 0; i < l.size(); i++) {
					if (i != 0) {
						sb.append(",");
					}
					sb.append("[");
					sb.append(l.get(i));
					sb.append("]");
				}

				return sb.toString();
			}
		}
	}

	/**
	 * FA type.
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class FA {
		public Bar[] l;

		@Override
		public String toString() {
			if (l == null) {
				return "null";
			} else {
				StringBuilder sb = new StringBuilder();

				for (int i = 0; i < l.length; i++) {
					if (i != 0) {
						sb.append(",");
					}
					sb.append("[");
					sb.append(l[i]);
					sb.append("]");
				}

				return sb.toString();
			}
		}
	}

	/**
	 * FM type.
	 */
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class FM {
		public Map<String, Bar> m;

		@Override
		public String toString() {
			if (m == null) {
				return "null";
			} else {
				StringBuilder sb = new StringBuilder();

				Iterator<Map.Entry<String, Bar>> iterator = m.entrySet().iterator();
				int i = 0;
				while (iterator.hasNext()) {
					if (i != 0) {
						sb.append(",");
					}

					sb.append("{");
					sb.append(iterator.next());
					sb.append("}");
					i += 1;
				}

				return sb.toString();
			}
		}
	}

}
