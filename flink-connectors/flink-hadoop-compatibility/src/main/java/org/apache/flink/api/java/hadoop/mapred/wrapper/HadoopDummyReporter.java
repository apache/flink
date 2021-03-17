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

package org.apache.flink.api.java.hadoop.mapred.wrapper;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

/** This is a dummy progress monitor / reporter. */
@PublicEvolving
public class HadoopDummyReporter implements Reporter {

    @Override
    public void progress() {}

    @Override
    public void setStatus(String status) {}

    @Override
    public Counter getCounter(Enum<?> name) {
        return null;
    }

    @Override
    public Counter getCounter(String group, String name) {
        return null;
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {}

    @Override
    public void incrCounter(String group, String counter, long amount) {}

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return null;
    }

    // There should be an @Override, but some CDH4 dependency does not contain this method
    public float getProgress() {
        return 0;
    }
}
