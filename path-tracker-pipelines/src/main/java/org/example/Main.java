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

package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.xml.crypto.Data;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Main {

    // topic for queues
    private static String OUTPUT_TOPIC = "test-output-topic";
    public static void main(String[] args) throws  Exception{

        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();

        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        AdminClient adminClient = KafkaAdminClient.create(prop);

        StreamExecutionEnvironment env = createPipeline(kafka.getBootstrapServers());

        int pathNum = PathAnalyzer.computePathNum(env);

        NewTopic newTopic = new NewTopic(OUTPUT_TOPIC, pathNum, (short) 1);
        adminClient.createTopics(Collections.singleton(newTopic));


        env.execute();
        

    }

    private static StreamExecutionEnvironment createPipeline(String kafkaBootstrapServer) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSink<DataRecord> kafkaSink = KafkaSink.<DataRecord>builder()
                .setBootstrapServers(kafkaBootstrapServer)
                .setRecordSerializer(new CustomKafkaSerializer(OUTPUT_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.addSource(new TestDataSource(100)).setParallelism(1)
                // filter out multiples of 7
                .filter((DataRecord x) -> { return x.getValue()%7 != 0; }).setParallelism(3)
                .rescale()
                // multiply by 2
                .map((DataRecord x) -> {x.setValue(x.getValue()*2); return x; }).setParallelism(4)
                .keyBy(DataRecord::getValue)
                // square it
                .map((DataRecord x) -> {int newValue = x.getValue()*x.getValue(); x.setValue(newValue); return x; }).setParallelism(2)
                .sinkTo(kafkaSink).setParallelism(1);



        return env;

    }
}



class DataRecord {

    private int sequenceId;

    private int value;



    public DataRecord(int sequenceId){
        this.sequenceId = sequenceId;

        Random r = new Random();
        this.value = r.nextInt(100);
    }

    public void setSequenceId(int sequenceId){
        this.sequenceId = sequenceId;

    }

    public int getSequenceId(){
        return this.sequenceId;
    }

    public void setValue(int value){
        this.value = value;
    }

    public int getValue(){
        return this.value;
    }

}

class TestDataSource extends RichSourceFunction<DataRecord> {
    private boolean running = true;

    private boolean isInfiniteSource = true;
    private long recordsPerInvocation = 0L;

    public TestDataSource(){}

    public TestDataSource(long recordsPerInvocation){
        this.recordsPerInvocation = recordsPerInvocation;
        this.isInfiniteSource = false;
    }

    @Override
    public void run(SourceContext<DataRecord> sourceContext) throws Exception {
        int counter = 0;

        long recordsRemaining = this.recordsPerInvocation;
        while(isInfiniteSource || recordsRemaining > 0){

            sourceContext.collect(new DataRecord(counter++));

            if (!isInfiniteSource){
                recordsRemaining--;
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
