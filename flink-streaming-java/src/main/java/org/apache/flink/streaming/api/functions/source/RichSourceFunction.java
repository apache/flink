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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.AbstractRichFunction;

/**
 * Base class for implementing a parallel data source that has access to context information
 * (via {@link #getRuntimeContext()}) and additional life-cycle methods
 * ({@link #open(org.apache.flink.configuration.Configuration)} and {@link #close()}.
 *
 * <p>This class is useful when implementing parallel sources where different parallel subtasks
 * need to perform different work. Typical patterns for that are:
 * <ul>
 *     <li>Use {@link #getRuntimeContext()} to obtain the runtime context.</li>
 *     <li>Use {@link org.apache.flink.api.common.functions.RuntimeContext#getNumberOfParallelSubtasks()}
 *         to determine the current parallelism. It is strongly encouraged to use this method, rather than
 *         hard-wiring the parallelism, because the configured parallelism may change depending on
 *         program configuration. The parallelism may also change after recovering failures, when fewer than
 *         desired parallel worker as available.</li>
 *     <li>Use {@link org.apache.flink.api.common.functions.RuntimeContext#getIndexOfThisSubtask()} to
 *         determine which subtask the current instance of the function executes.</li>
 * </ul>
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Public
public abstract class RichSourceFunction<OUT> extends AbstractRichFunction implements SourceFunction<OUT> {

	private static final long serialVersionUID = 1L;
}
