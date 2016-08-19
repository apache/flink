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
package org.apache.flink.cep;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Map;

@SuppressWarnings("serial")
public class CEPComplexPatternsITCase extends CEPITCaseBase {

	@Test
	public void testOneOrMoreCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0),
			new Event(3, "middle", 3.0),
			new SubEvent(4, "middle", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").oneOrMany().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle_0").getId()).append(",")
					.append(pattern.get("middle_1").getId()).append(",")
					.append(pattern.get("middle_2").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,3,4,5,8";

		env.execute();
	}

	@Test
	public void testOneOrMoreAtTheBeginningCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 2.0),
			new Event(2, "start", 2.0),
			new Event(3, "start", 2.0),
			new Event(4, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").oneOrMany().where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});
		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				for (String name : new String[] {"start", "start_0", "start_1", "start_2"}) {
					if (pattern.containsKey(name)) {
						builder.append(pattern.get(name).getId()).append(",");
					}
				}
				builder.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "3,4\n2,3,4\n1,2,3,4";

		env.execute();
	}

	@Test
	public void testOptionalCEPPatternWhenEventNotReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 2.0),
			new Event(2, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").optional().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "1,2";

		env.execute();
	}

	@Test
	public void testOptionalCEPPatternWhenEventReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 2.0),
			new Event(2, "middle", 2.0),
			new Event(3, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").optional().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "1,2,3";

		env.execute();
	}

	@Test
	public void testOptionalAtTheBeginningOfCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "middle", 2.0),
			new Event(2, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").optional().where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "1,2";

		env.execute();
	}

	@Test
	public void testOptionalAtTheEndOfCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 2.0),
			new Event(2, "middle", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").optional().where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "1,2";

		env.execute();
	}

	@Test
	public void testSeveralOptionalsInTheRowEndOfCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").optional().where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").optional().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2";

		env.execute();
	}

	@Test
	public void testZeroOrMoreCEPPatternWhenEventReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0),
			new Event(3, "middle", 3.0),
			new SubEvent(4, "middle", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").zeroOrMany().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle_0").getId()).append(",")
					.append(pattern.get("middle_1").getId()).append(",")
					.append(pattern.get("middle_2").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,3,4,5,8";

		env.execute();
	}

	@Test
	public void testZeroOrMoreCEPPatternWhenEventNotReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 2.0),
			new Event(2, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").zeroOrMany().where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "1,2";

		env.execute();
	}

	@Test
	public void testFixedCountCEPPattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0),
			new Event(3, "middle", 3.0),
			new SubEvent(4, "middle", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").count(3).where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("middle#1").getId()).append(",")
					.append(pattern.get("middle#2").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,3,4,5,8";

		env.execute();
	}

	@Test
	public void testMinMaxCountCEPPatternWhenMinNumberOfEventsReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0),
			new Event(3, "middle", 3.0),
			new SubEvent(4, "middle", 4.0, 1.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").count(2, 4).where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("middle#1").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,3,4,8";

		env.execute();
	}

	@Test
	public void testMinMaxCountCEPPatternWhenMaxNumberOfEventsReceived() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0),
			new Event(3, "middle", 3.0),
			new SubEvent(4, "middle", 4.0, 1.0),
			new SubEvent(5, "middle", 4.0, 1.0),
			new SubEvent(6, "middle", 4.0, 1.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").count(2, 4).where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("middle#1").getId()).append(",")
					.append(pattern.get("middle#2").getId()).append(",")
					.append(pattern.get("middle#3").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,3,4,5,6,8";

		env.execute();
	}

	@Test
	public void testZeroCountCEPPatternConsideredOptional() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(2, "start", 2.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new FilterFunction<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		)
			.next("middle").count(0, 3).where(
				new FilterFunction<Event>() {

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.next("end").optional().where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2";

		env.execute();
	}

}
