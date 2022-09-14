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

package org.apache.flink.test.windowing.sessionwindows;

/**
 * Basic interface for event generators.
 *
 * @param <K> session key type
 * @param <E> session event type
 */
public interface EventGenerator<K, E> {

    /**
     * Only call this method if hasMoreEvents() is true and canGenerateEventAtWatermark(...) is
     * true.
     *
     * @param globalWatermark the current global watermark to consider for event generation
     * @return a generated event
     */
    E generateEvent(long globalWatermark);

    /**
     * @param globalWatermark the current global watermark to consider for event generation
     * @return true if, given the current global watermark, the generator can produce events. this
     *     might temporarily be false, e.g. if a generator wants to produce late events w.r.t. a
     *     session, but the global watermark did not yet advance to the point that timestamps from
     *     the session would be late
     */
    boolean canGenerateEventAtWatermark(long globalWatermark);

    /** @return true if, in general and for some global watermark, more events can been produced */
    boolean hasMoreEvents();

    /** @return the watermark that tracks this generator's progress */
    long getLocalWatermark();

    /**
     * @param globalWatermark the current global watermark to consider for event generation
     * @return a successor for this generator if hasMoreEvents() is false
     */
    EventGenerator<K, E> getNextGenerator(long globalWatermark);

    /** @return key for the generated events */
    K getKey();
}
