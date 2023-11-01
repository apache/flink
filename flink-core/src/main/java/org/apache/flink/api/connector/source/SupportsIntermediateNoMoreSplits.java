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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Internal;

/**
 * A decorative interface of {@link SplitEnumeratorContext} which allows to handle intermediate
 * NoMoreSplits.
 *
 * <p>The split enumerator must implement this interface if it needs to deal with NoMoreSplits event
 * in cases of a subtask can have multiple child sources. e.g. hybrid source.
 */
@Internal
public interface SupportsIntermediateNoMoreSplits {
    /**
     * Signals a subtask that it will not receive split for current source, but it will receive
     * split for next sources. A common scenario is HybridSource. This indicates that task not truly
     * read finished.
     *
     * @param subtask The index of the operator's parallel subtask that shall be signaled it will
     *     receive splits later.
     */
    void signalIntermediateNoMoreSplits(int subtask);
}
