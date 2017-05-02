/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A timestamp assigner and watermark generator for streams where timestamps are monotonously
 * ascending. In this case, the local watermarks for the streams are easy to generate, because
 * they strictly follow the timestamps.
 *
 * <p><b>Note:</b> This is just a deprecated stub class. The actual code for this has moved to
 * {@link org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor}.
 *
 * @param <T> The type of the elements that this function can extract timestamps from
 *
 * @deprecated Extend {@link org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor} instead.
 */
@PublicEvolving
@Deprecated
public abstract class AscendingTimestampExtractor<T>
	extends org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<T> {

}
