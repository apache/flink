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

package org.apache.flink.streaming.api.iteration;

import java.io.Serializable;

/**
 * Predicate that defines the end of the iteration.
 * If {@link EndOfIterationPredicate#isEndOfIteration(T)} returns true for a value
 * at the iteration head, the instance of iteration will not wait for new values.
 *
 * @param <T>
 *     Type of the iteration input.
 */
public interface EndOfIterationPredicate<T> extends Serializable {

	boolean isEndOfIteration(T nextElement);

}
