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

package org.apache.flink.test.util;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.core.io.InputStatus;

/** Wraps the actual {@link RateLimitedSourceReader} to send generalized watermarks. */
@Experimental
public class MockSourceReaderWithGeneralizedWatermark<E, SplitT extends SourceSplit>
        extends RateLimitedSourceReader<E, SplitT> {

    private final WatermarkSupplier watermarkSupplier;

    public MockSourceReaderWithGeneralizedWatermark(
            SourceReader<E, SplitT> sourceReader,
            RateLimiter rateLimiter,
            WatermarkSupplier watermarkSupplier) {
        super(sourceReader, rateLimiter);
        this.watermarkSupplier = watermarkSupplier;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<E> output) throws Exception {
        InputStatus status = super.pollNext(output);
        watermarkSupplier.getWatermark().ifPresent(output::emitWatermark);
        return status;
    }
}
