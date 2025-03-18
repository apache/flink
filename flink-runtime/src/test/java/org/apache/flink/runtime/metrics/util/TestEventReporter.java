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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.events.Event;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.events.reporter.EventReporterFactory;
import org.apache.flink.metrics.MetricConfig;

import java.util.Properties;

/** Test implementation of {@link EventReporter}. */
public class TestEventReporter implements EventReporter, EventReporterFactory {

    @Override
    public void notifyOfAddedEvent(Event event) {}

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public EventReporter createEventReporter(Properties properties) {
        return this;
    }
}
