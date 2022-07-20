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
 *
 */

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.util.List;

/**
 * The {@code Committer} is responsible for committing the data staged by the sink.
 *
 * @param <CommT> The type of information needed to commit the staged data
 * @deprecated Please use {@link org.apache.flink.api.connector.sink2.Committer}.
 */
@Deprecated
@PublicEvolving
public interface Committer<CommT> extends AutoCloseable {

    /**
     * Commits the given list of {@link CommT} and returns a list of {@link CommT} that need to be
     * re-committed. The elements of the return list must be a subset of the input list, so that
     * successful committables can be inferred.
     *
     * @param committables A list of information needed to commit data staged by the sink.
     * @return a list of {@link CommT} that need to be re-committed.
     * @throws IOException if the commit operation fail and do not want to retry any more.
     */
    List<CommT> commit(List<CommT> committables) throws IOException, InterruptedException;
}
