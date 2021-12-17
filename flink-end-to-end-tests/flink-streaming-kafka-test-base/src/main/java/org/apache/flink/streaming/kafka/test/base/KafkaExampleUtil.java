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

package org.apache.flink.streaming.kafka.test.base;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** The util class for kafka example. */
public class KafkaExampleUtil {

    public static StreamExecutionEnvironment prepareExecutionEnv(ParameterTool parameterTool)
            throws Exception {

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
                            + "--bootstrap.servers <kafka brokers> "
                            + "--group.id <some id>");
            throw new Exception(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
                            + "--bootstrap.servers <kafka brokers> "
                            + "--group.id <some id>");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig()
                .setGlobalJobParameters(
                        parameterTool); // make parameters available in the web interface

        return env;
    }
}
