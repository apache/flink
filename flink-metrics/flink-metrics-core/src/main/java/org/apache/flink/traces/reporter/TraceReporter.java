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

package org.apache.flink.traces.reporter;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.traces.Span;

/**
 * Trace reporters are used to export {@link Span Spans} to an external backend.
 *
 * <p>Reporters are instantiated via a {@link TraceReporterFactory}.
 */
@Experimental
public interface TraceReporter {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * Configures this reporter.
     *
     * <p>If the reporter was instantiated generically and hence parameter-less, this method is the
     * place where the reporter sets it's basic fields based on configuration values. Otherwise,
     * this method will typically be a no-op since resources can be acquired in the constructor.
     *
     * <p>This method is always called first on a newly instantiated reporter.
     *
     * @param config A properties object that contains all parameters set for this reporter.
     */
    void open(MetricConfig config);

    /** Closes this reporter. Should be used to close channels, streams and release resources. */
    void close();

    void notifyOfAddedSpan(Span span);
}
