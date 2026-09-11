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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;

/**
 * Optional capability for streams that support immediate resource release from an external thread.
 *
 * <p>Implemented by SDK streams that can unblock a concurrent {@code read()} or {@code write()}.
 * Discovered via {@code instanceof}.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>May be called concurrently with an in-progress {@code read()} or {@code write()}.
 *   <li>Must never throw.
 *   <li>Must be idempotent.
 *   <li>Does not replace {@code close()}.
 * </ul>
 */
@Experimental
public interface Abortable {

    /**
     * Releases the stream's resources immediately, unblocking any blocked {@code read()} or {@code
     * write()}.
     */
    void abort();
}
