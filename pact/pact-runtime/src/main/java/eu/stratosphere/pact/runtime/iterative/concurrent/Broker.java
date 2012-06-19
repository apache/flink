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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

/** A concurrent datastructure that allows the handover of an object between a pair of threads*/
public class Broker<K, V> {

  private final ConcurrentMap<K, HandOver<V>> mediations = Maps.newConcurrentMap();

  /** threadsafe call to get a shared {@link HandOver} object */
  public HandOver<V> mediate(K key) {
    HandOver<V> handOver = new HandOver<V>();
    HandOver<V> commonHandOver = mediations.putIfAbsent(key, handOver);
    return commonHandOver != null ? commonHandOver : handOver;
  }

  /** remove {@link HandOver} object after successful delivery */
  public void notifyHandOverDone(K key) {
    mediations.remove(key);
  }

  /** can be used for a blocking hand over of an object to share */
  static class HandOver<V> {

    private final BlockingQueue<V> queue = new ArrayBlockingQueue<V>(1);

    public void handIn(V obj) {
      queue.offer(obj);
    }

    public V get() {
      try {
        return queue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
