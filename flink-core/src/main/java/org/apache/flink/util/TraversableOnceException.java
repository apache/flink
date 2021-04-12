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

package org.apache.flink.util;

import org.apache.flink.annotation.Public;

/**
 * An exception, indicating that an {@link java.lang.Iterable} can only be traversed once, but has
 * been attempted to traverse an additional time.
 */
@Public
public class TraversableOnceException extends RuntimeException {

    private static final long serialVersionUID = 7636881584773577290L;

    /** Creates a new exception with a default message. */
    public TraversableOnceException() {
        super(
                "The Iterable can be iterated over only once. Only the first call to 'iterator()' will succeed.");
    }
}
