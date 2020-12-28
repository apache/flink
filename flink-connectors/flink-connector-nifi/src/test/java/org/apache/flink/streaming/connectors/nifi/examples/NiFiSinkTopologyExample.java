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

package org.apache.flink.streaming.connectors.nifi.examples;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacketBuilder;
import org.apache.flink.streaming.connectors.nifi.NiFiSink;
import org.apache.flink.streaming.connectors.nifi.StandardNiFiDataPacket;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import java.util.HashMap;

/** An example topology that sends data to a NiFi input port named "Data from Flink". */
public class NiFiSinkTopologyExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SiteToSiteClientConfig clientConfig =
                new SiteToSiteClient.Builder()
                        .url("http://localhost:8080/nifi")
                        .portName("Data from Flink")
                        .buildConfig();

        DataStreamSink<String> dataStream =
                env.fromElements("one", "two", "three", "four", "five", "q")
                        .addSink(
                                new NiFiSink<>(
                                        clientConfig,
                                        new NiFiDataPacketBuilder<String>() {
                                            @Override
                                            public NiFiDataPacket createNiFiDataPacket(
                                                    String s, RuntimeContext ctx) {
                                                return new StandardNiFiDataPacket(
                                                        s.getBytes(ConfigConstants.DEFAULT_CHARSET),
                                                        new HashMap<String, String>());
                                            }
                                        }));

        env.execute();
    }
}
