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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiSource;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import java.nio.charset.Charset;

/** An example topology that pulls data from a NiFi output port named "Data for Flink". */
public class NiFiSourceTopologyExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SiteToSiteClientConfig clientConfig =
                new SiteToSiteClient.Builder()
                        .url("http://localhost:8080/nifi")
                        .portName("Data for Flink")
                        .requestBatchCount(5)
                        .buildConfig();

        SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);
        DataStream<NiFiDataPacket> streamSource = env.addSource(nifiSource).setParallelism(2);

        DataStream<String> dataStream =
                streamSource.map(
                        new MapFunction<NiFiDataPacket, String>() {
                            @Override
                            public String map(NiFiDataPacket value) throws Exception {
                                return new String(value.getContent(), Charset.defaultCharset());
                            }
                        });

        dataStream.print();
        env.execute();
    }
}
