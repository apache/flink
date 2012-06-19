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

import com.google.common.collect.Maps;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class BrokerTest {

  @Test
  public void mediation() throws InterruptedException {
    Random random = new Random();
    for (int n = 0; n < 20; n++) {
      mediate(random.nextInt(10) + 1);
    }
  }

  void mediate(int subtasks) throws InterruptedException {

    Broker<Integer, String> broker = new Broker<Integer, String>();
    ConcurrentMap<Integer, String> results = Maps.newConcurrentMap();

    Thread[] heads = new Thread[subtasks];
    for (int subtask = 0; subtask < subtasks; subtask++) {
      heads[subtask] = new Thread(new IterationHead(broker, subtask, "value" + subtask));
    }

    Thread[] tails = new Thread[subtasks];
    for (int subtask = 0; subtask < subtasks; subtask++) {
      tails[subtask] = new Thread(new IterationTail(broker, subtask, results));
    }

    for (int subtask = 0; subtask < subtasks; subtask++) {
      heads[subtask].start();
      tails[subtask].start();
    }

    for (int subtask = 0; subtask < subtasks; subtask++) {
      heads[subtask].join();
      tails[subtask].join();
    }

    // every tail must have gotten its handover value
    assertEquals(subtasks, results.size());
    for (int subtask = 0; subtask < subtasks; subtask++) {
      // every tail must have gotten its correct handover value
      assertEquals("value" + subtask, results.get(subtask));
    }
  }

  class IterationHead implements Runnable {

    private final Random random;
    private final Broker<Integer, String> broker;
    private final Integer key;
    private final String value;

    IterationHead(Broker<Integer, String> broker, Integer key, String value) {
      this.broker = broker;
      this.key = key;
      this.value = value;
      random = new Random();
    }

    @Override
    public void run() {
      try {
        Thread.sleep(random.nextInt(10));
        System.out.println("Head " + key + " asks for handover");
        Broker.HandOver<String> handOver = broker.mediate(key);
        Thread.sleep(random.nextInt(10));
        System.out.println("Head " + key + " hands in " + value);
        handOver.handIn(value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  class IterationTail implements Runnable {

    private final Random random;
    private final Broker<Integer, String> broker;
    private final Integer key;
    private final ConcurrentMap<Integer, String> results;

    IterationTail(Broker<Integer, String> broker, Integer key, ConcurrentMap<Integer, String> results) {
      this.broker = broker;
      this.key = key;
      this.results = results;
      random = new Random();
    }

    @Override
    public void run() {
      try {
        Thread.sleep(random.nextInt(10));
        System.out.println("Tail " + key + " asks for handover");
        Broker.HandOver<String> handOver = broker.mediate(key);
        Thread.sleep(random.nextInt(10));
        String value = handOver.get();
        Thread.sleep(random.nextInt(10));
        broker.notifyHandOverDone(key);
        System.out.println("Tail " + key + " received " + value);
        results.put(key, value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


}
