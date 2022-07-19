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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;

/**
 * Sources that implement this interface can allow dynamic initialization for the next source during
 * sources switch when using {@link HybridSource}.
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the source splits.
 */
@PublicEvolving
public interface DynamicHybridSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {
    /**
     * Gets a list of finished splits for this reader.
     *
     * <p>This method is called by {@link HybridSourceReader} during checkpoint for persistent, and
     * source switch to dynamically initiate the start positions of the next source.
     *
     * <p><b>Important:</b> The contract is that implementation should ensure the new finished
     * splits since the last call of this method persist until the next checkpoint.
     */
    List<SplitT> getFinishedSplits();
}
