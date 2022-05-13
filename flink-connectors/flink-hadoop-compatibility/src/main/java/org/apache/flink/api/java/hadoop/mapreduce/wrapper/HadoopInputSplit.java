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

package org.apache.flink.api.java.hadoop.mapreduce.wrapper;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A wrapper that represents an input split from the Hadoop mapreduce API as a Flink {@link
 * InputSplit}.
 */
public class HadoopInputSplit extends LocatableInputSplit {

    private static final long serialVersionUID = 6119153593707857235L;

    private final Class<? extends org.apache.hadoop.mapreduce.InputSplit> splitType;

    private transient org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit;

    public HadoopInputSplit(
            int splitNumber,
            org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit,
            JobContext jobContext) {
        super(splitNumber, (String) null);

        if (mapreduceInputSplit == null) {
            throw new NullPointerException("Hadoop input split must not be null");
        }
        if (!(mapreduceInputSplit instanceof Writable)) {
            throw new IllegalArgumentException("InputSplit must implement Writable interface.");
        }
        this.splitType = mapreduceInputSplit.getClass();
        this.mapreduceInputSplit = mapreduceInputSplit;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public org.apache.hadoop.mapreduce.InputSplit getHadoopInputSplit() {
        return mapreduceInputSplit;
    }

    @Override
    public String[] getHostnames() {
        try {
            return mapreduceInputSplit.getLocations();
        } catch (Exception e) {
            return new String[0];
        }
    }

    // ------------------------------------------------------------------------
    //  Serialization
    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        // serialize the parent fields and the final fields
        out.defaultWriteObject();

        // write the input split
        ((Writable) mapreduceInputSplit).write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        // read the parent fields and the final fields
        in.defaultReadObject();

        try {
            Class<? extends Writable> writableSplit = splitType.asSubclass(Writable.class);
            mapreduceInputSplit =
                    (org.apache.hadoop.mapreduce.InputSplit)
                            WritableFactories.newInstance(writableSplit);
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate the Hadoop InputSplit", e);
        }

        ((Writable) mapreduceInputSplit).readFields(in);
    }
}
