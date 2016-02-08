/**
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
package org.apache.flink.streaming.api.functions.source;


import org.apache.flink.annotation.PublicEvolving;

/**
 * A marker interface that must be implemented by {@link SourceFunction}s that emit elements with
 * timestamps. The {@link SourceFunction} can extract the timestamp from the data and attach it to
 * the element upon emission.
 *
 * <p>
 * Event-time sources must manually emit
 * {@link org.apache.flink.streaming.api.watermark.Watermark watermarks} to keep track of progress.
 * Automatic emission of watermarks will be suppressed if a source implements this interface.
 *
 * <p>
 * Elements must be emitted using
 * {@link SourceFunction.SourceContext#collectWithTimestamp(Object, long)}
 * and watermarks can be emitted using
 * {@link SourceFunction.SourceContext#emitWatermark(org.apache.flink.streaming.api.watermark.Watermark)}.
 *
 * @param <T> Type of the elements emitted by this source.
 */
@PublicEvolving
public interface EventTimeSourceFunction<T> extends SourceFunction<T> { }
