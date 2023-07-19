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

package org.apache.flink.examples.java.jointest.util;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

/** A bounded source used to generate data stream in batch mode. */
public class MyJoinGenerator implements GeneratorFunction<Long, String> {

    private Random random;
    private long numValues;
    private long initSeed;
    private int numPreGeneratedData;
    private String[] preGeneratedData;

    public MyJoinGenerator(long numValues, long initSeed) {
        this.numValues = numValues;
        this.initSeed = initSeed;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        GeneratorFunction.super.open(readerContext);
        random = new Random(initSeed);
        numPreGeneratedData = (int) numValues;
        preGeneratedData = new String[numPreGeneratedData];
        for (int i = 0; i < numPreGeneratedData; i++) {
            preGeneratedData[i] = Integer.toString(random.nextInt(numPreGeneratedData));
        }
    }

    @Override
    public String map(Long value) throws Exception {
        return preGeneratedData[(int) (value % numPreGeneratedData)];
    }
}
