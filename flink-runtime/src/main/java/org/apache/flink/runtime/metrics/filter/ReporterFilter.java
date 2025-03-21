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

package org.apache.flink.runtime.metrics.filter;

/** A filter for metrics, spans, or events. */
public interface ReporterFilter<T> {

    /** Filter that accepts every reported. */
    ReporterFilter<?> NO_OP_FILTER = (metric, name, scope) -> true;

    @SuppressWarnings("unchecked")
    static <T> ReporterFilter<T> getNoOpFilter() {
        return (ReporterFilter<T>) NO_OP_FILTER;
    }

    /**
     * Filters a given reported.
     *
     * @param reported the reported to filter
     * @param name the name of the reported
     * @param logicalScope the logical scope of the reported
     * @return true, if the reported matches, false otherwise
     */
    boolean filter(T reported, String name, String logicalScope);
}
