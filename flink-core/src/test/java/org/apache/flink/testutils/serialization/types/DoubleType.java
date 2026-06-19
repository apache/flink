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

package org.apache.flink.testutils.serialization.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Random;

public class DoubleType implements SerializationTestType {

    private double value;

    public DoubleType() {
        this.value = 0;
    }

    private DoubleType(double value) {
        this.value = value;
    }

    @Override
    public DoubleType getRandom(Random rnd) {
        return new DoubleType(rnd.nextDouble());
    }

    @Override
    public int length() {
        return 8;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeDouble(this.value);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.value = in.readDouble();
    }

    @Override
    public int hashCode() {
        final long l = Double.doubleToLongBits(this.value);
        return (int) (l ^ l >>> 32);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleType) {
            DoubleType other = (DoubleType) obj;
            return Double.doubleToLongBits(this.value) == Double.doubleToLongBits(other.value);
        } else {
            return false;
        }
    }
}
