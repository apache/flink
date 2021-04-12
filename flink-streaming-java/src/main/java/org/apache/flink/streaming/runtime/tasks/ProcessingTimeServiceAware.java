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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory}, which
 * enables stream operators to access {@link ProcessingTimeService}.
 *
 * <p>For an stream operator, if it needs to access the {@link ProcessingTimeService}, it must be
 * created through a operator factory that implements {@link ProcessingTimeServiceAware}, and the
 * operator factory should set the {@link ProcessingTimeService} instance into the stream operator.
 */
@PublicEvolving
public interface ProcessingTimeServiceAware {

    void setProcessingTimeService(ProcessingTimeService processingTimeService);
}
