/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

//TODO generify
public class IterationContext {

  private ConcurrentMap<Integer, AtomicLong> counts;

  /** single instance */
  private static final IterationContext INSTANCE = new IterationContext();

  private IterationContext() {
    counts = Maps.newConcurrentMap();
  }

  /** retrieve singleton instance */
  public static IterationContext instance() {
    return INSTANCE;
  }

  public void initCount(int index) {
    counts.put(index, new AtomicLong(0));
  }

  public void resetCount(int index) {
    counts.get(index).set(0);
  }


  public void incrementCount(int index, long delta) {
    counts.get(index).addAndGet(delta);
  }

  public long count(int index) {
    return counts.get(index).get();
  }



}
