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

package org.apache.flink.python;

/** constants. */
public class Constants {

    // function urns
    public static final String STATELESS_FUNCTION_URN = "flink:transform:ds:stateless_function:v1";
    public static final String STATEFUL_FUNCTION_URN = "flink:transform:ds:stateful_function:v1";

    // coder urns
    public static final String FLINK_CODER_URN = "flink:coder:v1";

    // execution graph
    public static final String TRANSFORM_ID = "transform";
    public static final String MAIN_INPUT_NAME = "input";
    public static final String MAIN_OUTPUT_NAME = "output";
    public static final String WINDOW_STRATEGY = "windowing_strategy";

    public static final String INPUT_COLLECTION_ID = "input";
    public static final String OUTPUT_COLLECTION_ID = "output";

    public static final String WINDOW_CODER_ID = "window_coder";
    public static final String TIMER_ID = "timer";
    public static final String TIMER_CODER_ID = "timer_coder";
    public static final String WRAPPER_TIMER_CODER_ID = "timer_coder_wrapper";
}
