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

package org.apache.flink.datastream.api.context;

import org.apache.flink.annotation.Experimental;

/**
 * A RuntimeContext contains information about the context in which process functions are executed.
 * Each parallel instance of the function will have a context through which it can access contextual
 * information, such as the current key and execution mode. Through this context, we can also
 * interact with the execution layer, such as getting state, emitting watermark, registering timer,
 * etc.
 */
@Experimental
public interface RuntimeContext {
    // TODO Introduce related methods in the subsequent RP.
}
