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

package org.apache.flink.streaming.api.connector.sink2;

import org.assertj.core.api.InstanceOfAssertFactory;

/** Custom assertions for Sink V2 related classes. */
public class SinkV2Assertions {
    @SuppressWarnings({"rawtypes"})
    public static <CommT>
            InstanceOfAssertFactory<CommittableWithLineage, CommittableWithLineageAssert<CommT>>
                    committableWithLineage() {
        return new InstanceOfAssertFactory<>(
                CommittableWithLineage.class, SinkV2Assertions::<CommT>assertThat);
    }

    @SuppressWarnings({"rawtypes"})
    public static <CommT>
            InstanceOfAssertFactory<CommittableSummary, CommittableSummaryAssert<CommT>>
                    committableSummary() {
        return new InstanceOfAssertFactory<>(
                CommittableSummary.class, SinkV2Assertions::<CommT>assertThat);
    }

    public static <CommT> CommittableSummaryAssert<CommT> assertThat(
            CommittableSummary<CommT> summary) {
        return new CommittableSummaryAssert<>(summary);
    }

    public static <CommT> CommittableWithLineageAssert<CommT> assertThat(
            CommittableWithLineage<CommT> committableWithLineage) {
        return new CommittableWithLineageAssert<>(committableWithLineage);
    }
}
