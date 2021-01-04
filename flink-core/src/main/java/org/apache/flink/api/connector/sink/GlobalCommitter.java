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

import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.util.List;

/**
 * The {@code GlobalCommitter} is responsible for creating and committing an aggregated committable,
 * which we call global committable (see {@link #combine}).
 *
 * <p>The {@code GlobalCommitter} runs with parallelism equal to 1.
 *
 * @param <CommT> The type of information needed to commit data staged by the sink
 * @param <GlobalCommT> The type of the aggregated committable
 */
@Experimental
public interface GlobalCommitter<CommT, GlobalCommT> extends AutoCloseable {

    /**
     * Find out which global committables need to be retried when recovering from the failure.
     *
     * @param globalCommittables A list of {@link GlobalCommT} for which we want to verify which
     *     ones were successfully committed and which ones did not.
     * @return A list of {@link GlobalCommT} that should be committed again.
     * @throws IOException if fail to filter the recovered committables.
     */
    List<GlobalCommT> filterRecoveredCommittables(List<GlobalCommT> globalCommittables)
            throws IOException;

    /**
     * Compute an aggregated committable from a list of committables.
     *
     * @param committables A list of {@link CommT} to be combined into a {@link GlobalCommT}.
     * @return an aggregated committable
     * @throws IOException if fail to combine the given committables.
     */
    GlobalCommT combine(List<CommT> committables) throws IOException;

    /**
     * Commit the given list of {@link GlobalCommT}.
     *
     * @param globalCommittables a list of {@link GlobalCommT}.
     * @return A list of {@link GlobalCommT} needed to re-commit, which is needed in case we
     *     implement a "commit-with-retry" pattern.
     * @throws IOException if the commit operation fail and do not want to retry any more.
     */
    List<GlobalCommT> commit(List<GlobalCommT> globalCommittables) throws IOException;

    /**
     * Signals that there is no committable any more.
     *
     * @throws IOException if fail to handle this notification.
     */
    void endOfInput() throws IOException;
}
