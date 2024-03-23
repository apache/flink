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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class Main {
    public static void main(String[] args) throws  Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        DataRecord r1 = new DataRecord(1);
        DataRecord r2 = new DataRecord(2);
        env.addSource(new TestDataSource(100)).setParallelism(1)
                // filter out multiples of 7
                .filter((DataRecord x) -> { return x.getValue()%7 != 0; }).setParallelism(3)
                .rescale()
                // multiply by 2
                .map((DataRecord x) -> {x.setValue(x.getValue()*2); return x; }).setParallelism(4)
                .keyBy(DataRecord::getValue)
                // square it
                .map((DataRecord x) -> {return x.getValue()*x.getValue(); }).setParallelism(2)
                .print();

        env.execute();
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
