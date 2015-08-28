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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.Collection;
import java.util.Map;


public final class MatchRemovingJoiner implements FlatJoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>,Tuple2<Integer,String>> {
	private static final long serialVersionUID = 1L;

	private final Map<Integer, Collection<Match>> toRemoveFrom;

	public MatchRemovingJoiner(Map<Integer, Collection<Match>> map) {
		this.toRemoveFrom = map;
	}

	@Override
	public void join(Tuple2<Integer, String> rec1, Tuple2<Integer, String> rec2, Collector<Tuple2<Integer, String>> out) throws Exception {
		final Integer key = rec1 != null ? (Integer) rec1.getField(0) : (Integer) rec2.getField(0);
		final String value1 = rec1 != null ? (String) rec1.getField(1) : null;
		final String value2 = rec2 != null ? (String) rec2.getField(1) : null;

		Collection<Match> matches = this.toRemoveFrom.get(key);
		if (matches == null) {
			Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected.");
		}

		boolean contained = matches.remove(new Match(value1, value2));
		if (!contained) {
			Assert.fail("Produced match was not contained: " + key + " - " + value1 + ":" + value2);
		}
		if (matches.isEmpty()) {
			this.toRemoveFrom.remove(key);
		}
	}
}
