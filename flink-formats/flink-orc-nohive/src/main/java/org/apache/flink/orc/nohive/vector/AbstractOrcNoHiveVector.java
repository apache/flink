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

package org.apache.flink.orc.nohive.vector;

import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

/** This column vector is used to adapt hive's ColumnVector to Flink's ColumnVector. */
public abstract class AbstractOrcNoHiveVector
        implements org.apache.flink.table.data.columnar.vector.ColumnVector {

    private ColumnVector orcVector;

    AbstractOrcNoHiveVector(ColumnVector orcVector) {
        this.orcVector = orcVector;
    }

    @Override
    public boolean isNullAt(int i) {
        return !orcVector.noNulls && orcVector.isNull[orcVector.isRepeating ? 0 : i];
    }

    public static org.apache.flink.table.data.columnar.vector.ColumnVector createFlinkVector(
            ColumnVector vector) {
        if (vector instanceof LongColumnVector) {
            return new OrcNoHiveLongVector((LongColumnVector) vector);
        } else if (vector instanceof DoubleColumnVector) {
            return new OrcNoHiveDoubleVector((DoubleColumnVector) vector);
        } else if (vector instanceof BytesColumnVector) {
            return new OrcNoHiveBytesVector((BytesColumnVector) vector);
        } else if (vector instanceof DecimalColumnVector) {
            return new OrcNoHiveDecimalVector((DecimalColumnVector) vector);
        } else if (vector instanceof TimestampColumnVector) {
            return new OrcNoHiveTimestampVector((TimestampColumnVector) vector);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported vector: " + vector.getClass().getName());
        }
    }
}
