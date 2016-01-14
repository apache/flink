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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

public class PatternTest extends TestLogger {
	@Test
	public void testStrictContiguity() {
		Pattern<Object, ?> pattern = Pattern.begin("start").next("next").next("end");
	}

	@Test
	public void testNonStrictContiguity() {
		Pattern<Object, ?> pattern = Pattern.begin("start").followedBy("next").followedBy("end");
	}

	@Test
	public void testStrictContiguityWithCondition() {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").next("next").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -7657256242101104925L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("foobar");
			}
		}).next("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -7597452389191504189L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getId() == 42;
			}
		});
	}

	@Test
	public void testPatternWithSubtyping() {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").next("subevent").subtype(SubEvent.class).followedBy("end");
	}

	@Test
	public void testPatternWithSubtypingAndFilter() {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").next("subevent").subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
			private static final long serialVersionUID = -4118591291880230304L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return false;
			}
		}).followedBy("end");
	}
}
