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

package org.apache.flink.contrib.streaming.windowing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestCollector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.junit.Test;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

// As the MedianGroupedPreReducer uses the non-grouped MedianPreReducer, this also tests that class.
public class MedianGroupedPreReducerTest {

	TypeInformation<Tuple2<Integer, Double>> typeInfo = TypeExtractor.getForObject(new Tuple2<Integer, Double>(1, 1.0));

	@SuppressWarnings("unchecked")
	@Test
	public void testMedianPreReducer() throws Exception {
		Random rnd = new Random(42);

		ArrayDeque<Tuple2<Integer, Double> > inputs = new ArrayDeque<Tuple2<Integer, Double> >();

		KeySelector<Tuple2<Integer,Double>, Integer> keySelector = new KeySelector<Tuple2<Integer,Double>, Integer>() {
			@Override
			public Integer getKey(Tuple2<Integer, Double> value) throws Exception {
				return value.f0;
			}
		};
		MedianGroupedPreReducer<Tuple2<Integer, Double>> preReducer =
				new MedianGroupedPreReducer<Tuple2<Integer, Double>>(1, typeInfo, null, keySelector);

		TestCollector<StreamWindow<Tuple2<Integer, Double>>> collector =
				new TestCollector<StreamWindow<Tuple2<Integer, Double>>>();

		List<StreamWindow<Tuple2<Integer, Double>>> collected = collector.getCollected();

		// We do a bunch of random operations, and check the results by also naively calculating the median.
		int N = 10000;
		for(int i = 0; i < N; i++) {
			switch (rnd.nextInt(3)) {
				case 0: // store
					Tuple2<Integer, Double> elem;
					Integer group =  rnd.nextInt(5);
					// We sometimes add doubles and sometimes add small integers.
					// The latter is for ensuring that it happens that there are duplicate elements,
					// so that we test TreeMultiset properly.
					if(rnd.nextDouble() < 0.5) {
						elem = new Tuple2<Integer, Double>(group, (double)rnd.nextInt(5));
					} else {
						elem = new Tuple2<Integer, Double>(group, rnd.nextDouble());
					}
					inputs.addLast(elem);
					preReducer.store(elem);
					break;
				case 1: // evict
					int howMany = rnd.nextInt(2) + 1;
					int howMany2 = Math.min(howMany, inputs.size());
					for(int j = 0; j < howMany2; j++) {
						inputs.removeFirst();
					}
					preReducer.evict(howMany);
					break;
				case 2: // emitWindow
					if(inputs.size() > 0) {
						// Calculate the correct medians:
						// The inputs are split into groups into inputDoublesPerGroup,
						// then the medians are calculated per group into correctMedians.
						TreeMap<Integer, ArrayList<Double>> inputDoublesPerGroup = new TreeMap<Integer, ArrayList<Double>>();
						for (Tuple2<Integer, Double> e : inputs) {
							Integer key = keySelector.getKey(e);
							ArrayList<Double> a = inputDoublesPerGroup.get(key);
							if(a == null) { // Group doesn't exist yet, create it.
								a = new ArrayList<Double>();
								inputDoublesPerGroup.put(key, a);
							}
							a.add(e.f1);
						}
						TreeMap<Integer, Double> correctMedians = new TreeMap<Integer, Double>();
						for(Entry<Integer, ArrayList<Double>> e: inputDoublesPerGroup.entrySet()) {
							ArrayList<Double> doublesInGroup = e.getValue();
							Collections.sort(doublesInGroup);
							int half = doublesInGroup.size() / 2;
							correctMedians.put(
									e.getKey(),
									doublesInGroup.size() % 2 == 1 ?
											doublesInGroup.get(half) :
											(doublesInGroup.get(half) + doublesInGroup.get(half - 1)) / 2);
						}

						preReducer.emitWindow(collector);
						for(Tuple2<Integer, Double> e: collected.get(collected.size() - 1)){
							assertEquals(
									correctMedians.get(keySelector.getKey(e)),
									e.f1);
						}
					}
					break;
			}
		}
	}
}
