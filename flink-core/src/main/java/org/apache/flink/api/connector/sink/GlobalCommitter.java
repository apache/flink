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

import java.util.List;

/**
 * The {@link GlobalCommitter} is responsible for committing an aggregated committable, which we called global committable.
 *
 * @param <CommT>         The type of the committable data
 * @param <GlobalCommT>   The type of the aggregated committable
 */
@Experimental
public interface GlobalCommitter<CommT, GlobalCommT> extends Committer<GlobalCommT> {

	/**
	 * Find out which global committables need to be retried when recovering from the failure.
	 * @param globalCommittables the global committable that are properly not committed in the previous attempt.
	 * @return the global committables that should be committed again.
	 */
	List<GlobalCommT> filterRecoveredCommittables(List<GlobalCommT> globalCommittables);

	/**
	 * Compute an aggregated committable from a collection of committables.
	 * @param committables a collection of committables that are needed to combine
	 * @return an aggregated committable
	 */
	GlobalCommT combine(List<CommT> committables);

	/**
	 * Commit the given collection of {@link GlobalCommT}.
	 * @param globalCommittables a collection of {@link GlobalCommT}.
	 * @return a collection of {@link GlobalCommT} that is needed to re-commit latter.
	 * @throws Exception if the commit operation fail and do not want to retry any more.
	 */
	List<GlobalCommT> commit(List<GlobalCommT> globalCommittables) throws Exception;

	/**
	 * There is no committable any more.
	 */
	void endOfInput();
}
