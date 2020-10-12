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
 * This interface is responsible for committing the data to the external system.
 *
 * @param <CommT> The type of the committable data.
 */
@Experimental
public interface Committer<CommT> extends AutoCloseable {

	/**
	 * Commit the given collection of {@link CommT}.
	 * @param committable the data needed to be committed.
	 * @return a collection of {@link CommT} that is needed to re-commit latter.
	 * @throws Exception if the commit operation fail and do not want to retry any more.
	 */
	List<CommT> commit(List<CommT> committable) throws Exception;
}
