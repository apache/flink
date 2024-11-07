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
 *
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;

/**
 * Lineage vertex for source which has {@link Boundedness} to indicate whether the data for the
 * source is bounded.
 */
@PublicEvolving
public interface SourceLineageVertex extends LineageVertex {
    /**
     * The boundedness for the source connector, users can get boundedness for each sources in the
     * lineage and determine the job execution mode with RuntimeExecutionMode.
     */
    Boundedness boundedness();
}
