/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class BrokerTest {

	@Test
	public void mediation() throws Exception {
		Random random = new Random();
		for (int n = 0; n < 20; n++) {
			mediate(random.nextInt(10) + 1);
		}
	}

	void mediate(int subtasks) throws InterruptedException, ExecutionException {

		ExecutorService executorService = Executors.newFixedThreadPool(subtasks * 2);

		List<Callable<StringPair>> tasks = Lists.newArrayList();
		Broker<String> broker = new Broker<String>();

		for (int subtask = 0; subtask < subtasks; subtask++) {
			tasks.add(new IterationHead(broker, subtask, "value" + subtask));
			tasks.add(new IterationTail(broker, subtask));
		}

		Collections.shuffle(tasks);

		int numSuccessfulHandovers = 0;
		for (Future<StringPair> future : executorService.invokeAll(tasks)) {
			StringPair stringPair = future.get();
			if (stringPair != null) {
				assertEquals("value" + stringPair.getFirst(), stringPair.getSecond());
				numSuccessfulHandovers++;
			}
		}

		assertEquals(subtasks, numSuccessfulHandovers);
	}

	class IterationHead implements Callable<StringPair> {

		private final Random random;

		private final Broker<String> broker;

		private final String key;

		private final String value;

		IterationHead(Broker<String> broker, Integer key, String value) {
			this.broker = broker;
			this.key = String.valueOf(key);
			this.value = value;
			random = new Random();
		}

		@Override
		public StringPair call() throws Exception {
			Thread.sleep(random.nextInt(10));
			// System.out.println("Head " + key + " hands in " + value);
			broker.handIn(key, value);
			Thread.sleep(random.nextInt(10));
			return null;
		}

	}

	class IterationTail implements Callable<StringPair> {

		private final Random random;

		private final Broker<String> broker;

		private final String key;

		IterationTail(Broker<String> broker, Integer key) {
			this.broker = broker;
			this.key = String.valueOf(key);
			random = new Random();
		}

		@Override
		public StringPair call() throws Exception {

			Thread.sleep(random.nextInt(10));
//			System.out.println("Tail " + key + " asks for handover");
			String value = broker.getAndRemove(key);

//			System.out.println("Tail " + key + " received " + value);
			Preconditions.checkNotNull(value);

			return new StringPair(key, value);
		}
	}

}
