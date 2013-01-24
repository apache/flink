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
public class Broker<V> {

  private final ConcurrentMap<String, BlockingQueue<V>> mediations = Maps.newConcurrentMap();

  /** hand in the object to share */
  public void handIn(String key, V obj) {
    retrieveSharedQueue(key).offer(obj);
  }

  /** blocking retrieval of the object to share */
  public V get(String key) {
    try {
      V objToShare = retrieveSharedQueue(key).take();
      mediations.remove(key);
      return objToShare;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** threadsafe call to get a shared {@link BlockingQueue} */
  private BlockingQueue<V> retrieveSharedQueue(String key) {
    BlockingQueue<V> queue = new ArrayBlockingQueue<V>(1);
    BlockingQueue<V> commonQueue = mediations.putIfAbsent(key, queue);
    return commonQueue != null ? commonQueue : queue;
  }
}
