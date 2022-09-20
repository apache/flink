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

package org.apache.flink.connector.pulsar.table.testutils;

import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** A mock topic Router for testing purposes only. */
public class MockTopicRouter implements TopicRouter<RowData> {

    private static final long serialVersionUID = 1316133122715449818L;

    @Override
    public String route(
            RowData rowData, String key, List<String> partitions, PulsarSinkContext context) {
        return "never-exist-topic";
    }
}
