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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.connectors.hive.HiveTablePartition;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An wrapper class that wraps info needed for a hadoop input split. Right now, it contains info
 * about the partition of the split.
 */
public class HiveTableInputSplit extends HadoopInputSplit {

    private static final long serialVersionUID = 1L;

    protected final HiveTablePartition hiveTablePartition;

    public HiveTableInputSplit(
            int splitNumber,
            InputSplit hInputSplit,
            JobConf jobconf,
            HiveTablePartition hiveTablePartition) {
        super(splitNumber, hInputSplit, jobconf);
        this.hiveTablePartition =
                checkNotNull(hiveTablePartition, "hiveTablePartition can not be null");
    }

    public HiveTablePartition getHiveTablePartition() {
        return hiveTablePartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        HiveTableInputSplit split = (HiveTableInputSplit) o;
        return Objects.equals(hiveTablePartition, split.hiveTablePartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hiveTablePartition);
    }

    @Override
    public String toString() {
        return "HiveTableInputSplit{" + "hiveTablePartition=" + hiveTablePartition + '}';
    }
}
