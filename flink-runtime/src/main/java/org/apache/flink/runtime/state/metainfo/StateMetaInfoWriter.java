/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Functional interface to write {@link StateMetaInfoSnapshot}.
 */
@FunctionalInterface
public interface StateMetaInfoWriter {

	/**
	 * Writes the given snapshot to the output view.
	 *
	 * @param snapshot the snapshot to write.
	 * @param outputView the output to write into.
	 * @throws IOException on write problems.
	 */
	void writeStateMetaInfoSnapshot(
		@Nonnull StateMetaInfoSnapshot snapshot,
		@Nonnull DataOutputView outputView) throws IOException;
}
