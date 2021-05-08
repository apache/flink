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
 * Base class for implementing a parallel data source. Upon execution, the runtime will execute as
 * many parallel instances of this function as configured parallelism of the source.
 *
 * <p>The data source has access to context information (such as the number of parallel instances of
 * the source, and which parallel instance the current instance is) via {@link
 * #getRuntimeContext()}. It also provides additional life-cycle methods ({@link
 * #open(org.apache.flink.configuration.Configuration)} and {@link #close()}.
 *
 * @param <OUT> The type of the records produced by this source.
 */
@Public
public abstract class RichParallelSourceFunction<OUT> extends AbstractRichFunction
        implements ParallelSourceFunction<OUT> {

    private static final long serialVersionUID = 1L;
}
