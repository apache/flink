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

package org.apache.flink.util.clock;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A clock that gives access to relative time, similar to System#nanoTime(), however the progress of
 * the relative time doesn't have to reflect the progress of a wall clock. Concrete classes can
 * specify a different contract in that regard. Returned time must be monotonically increasing.
 */
@PublicEvolving
public interface RelativeClock {
    /** Gets the current relative time, in milliseconds. */
    long relativeTimeMillis();

    /** Gets the current relative time, in nanoseconds. */
    long relativeTimeNanos();
}
