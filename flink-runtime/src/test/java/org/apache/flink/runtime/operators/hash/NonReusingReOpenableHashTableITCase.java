/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.hash;

import org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase.TupleMatch;
import org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase.TupleMatchRemovingJoin;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import static org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase.joinTuples;
import static org.apache.flink.runtime.operators.hash.NonReusingHashJoinIteratorITCase.collectTupleData;

/**
 * Test specialized hash join that keeps the build side data (in memory and on hard disk)
 * This is used for iterative tasks.
 */
public class NonReusingReOpenableHashTableITCase extends ReOpenableHashTableTestBase {

	protected void doTest(TestData.TupleGeneratorIterator buildInput, TestData.TupleGeneratorIterator probeInput, TupleGenerator bgen, TupleGenerator pgen) throws Exception {
		// collect expected data
		final Map<Integer, Collection<TupleMatch>> expectedFirstMatchesMap = joinTuples(collectTupleData(buildInput), collectTupleData(probeInput));

		final List<Map<Integer, Collection<TupleMatch>>> expectedNMatchesMapList = new ArrayList<>(NUM_PROBES);
		final FlatJoinFunction[] nMatcher = new TupleMatchRemovingJoin[NUM_PROBES];
		for(int i = 0; i < NUM_PROBES; i++) {
			Map<Integer, Collection<TupleMatch>> tmp;
			expectedNMatchesMapList.add(tmp = deepCopy(expectedFirstMatchesMap));
			nMatcher[i] = new TupleMatchRemovingJoin(tmp);
		}

		final FlatJoinFunction firstMatcher = new TupleMatchRemovingJoin(expectedFirstMatchesMap);

		final Collector<Tuple2<Integer, String>> collector = new DiscardingOutputCollector<>();

		// reset the generators
		bgen.reset();
		pgen.reset();
		buildInput.reset();
		probeInput.reset();

		// compare with iterator values
		NonReusingBuildFirstReOpenableHashJoinIterator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> iterator =
				new NonReusingBuildFirstReOpenableHashJoinIterator<>(
						buildInput, probeInput, this.recordSerializer, this.record1Comparator,
					this.recordSerializer, this.record2Comparator, this.recordPairComparator,
					this.memoryManager, ioManager, this.parentTask, 1.0, false, false, true);

		iterator.open();
		// do first join with both inputs
		while (iterator.callWithNextKey(firstMatcher, collector));

		// assert that each expected match was seen for the first input
		for (Entry<Integer, Collection<TupleMatch>> entry : expectedFirstMatchesMap.entrySet()) {
			if (!entry.getValue().isEmpty()) {
				Assert.fail("Collection for key " + entry.getKey() + " is not empty");
			}
		}

		for(int i = 0; i < NUM_PROBES; i++) {
			pgen.reset();
			probeInput.reset();
			// prepare ..
			iterator.reopenProbe(probeInput);
			// .. and do second join
			while (iterator.callWithNextKey(nMatcher[i], collector));

			// assert that each expected match was seen for the second input
			for (Entry<Integer, Collection<TupleMatch>> entry : expectedNMatchesMapList.get(i).entrySet()) {
				if (!entry.getValue().isEmpty()) {
					Assert.fail("Collection for key " + entry.getKey() + " is not empty");
				}
			}
		}

		iterator.close();
	}


}
