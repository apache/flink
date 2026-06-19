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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Enables to fully rely on the watermark strategy provided by the {@link ScanTableSource} itself.
 *
 * <p>The concept of watermarks defines when time operations based on an event time attribute will
 * be triggered. A watermark tells operators that no elements with a timestamp older or equal to the
 * watermark timestamp should arrive at the operator. Thus, watermarks are a trade-off between
 * latency and completeness.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * CREATE TABLE t (i INT, ts TIMESTAMP(3), WATERMARK FOR ts AS SOURCE_WATERMARK())  // `ts` becomes a time attribute
 * }</pre>
 *
 * <p>In the above example, the {@code SOURCE_WATERMARK()} is a built-in marker function that will
 * be detected by the planner and translated into a call to this interface if available. If a source
 * does not implement this interface, an exception will be thrown.
 *
 * <p>Compared to {@link SupportsWatermarkPushDown}, it is not possible to influence a source's
 * watermark strategy using customs expressions if {@code SOURCE_WATERMARK()} is declared.
 * Nevertheless, a source can implement both interfaces if necessary.
 *
 * @see SupportsWatermarkPushDown
 */
@PublicEvolving
public interface SupportsSourceWatermark {

    /** Instructs the source to emit source-specific watermarks during runtime. */
    void applySourceWatermark();
}
