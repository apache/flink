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

/**
 * A stream data source that is executed in parallel. Upon execution, the runtime will
 * execute as many parallel instances of this function function as configured parallelism
 * of the source.
 *
 * <p>This interface acts only as a marker to tell the system that this source may
 * be executed in parallel. When different parallel instances are required to perform
 * different tasks, use the {@link RichParallelSourceFunction} to get access to the runtime
 * context, which reveals information like the number of parallel tasks, and which parallel
 * task the current instance is.
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Public
public interface ParallelSourceFunction<OUT> extends SourceFunction<OUT> {
}
