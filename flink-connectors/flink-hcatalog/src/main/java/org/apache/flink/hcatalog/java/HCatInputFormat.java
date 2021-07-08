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

package org.apache.flink.hcatalog.java;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.hcatalog.HCatInputFormatBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;

/**
 * A InputFormat to read from HCatalog tables. The InputFormat supports projection (selection and
 * order of fields) and partition filters.
 *
 * <p>Data can be returned as {@link HCatRecord} or Flink {@link
 * org.apache.flink.api.java.tuple.Tuple}. Flink tuples support only up to 25 fields.
 *
 * @param <T>
 */
public class HCatInputFormat<T> extends HCatInputFormatBase<T> {
    private static final long serialVersionUID = 1L;

    public HCatInputFormat() {}

    public HCatInputFormat(String database, String table) throws Exception {
        super(database, table);
    }

    public HCatInputFormat(String database, String table, Configuration config) throws Exception {
        super(database, table, config);
    }

    @Override
    protected int getMaxFlinkTupleSize() {
        return 25;
    }

    @Override
    protected T buildFlinkTuple(T t, HCatRecord record) throws HCatException {

        Tuple tuple = (Tuple) t;

        // Extract all fields from HCatRecord
        for (int i = 0; i < this.fieldNames.length; i++) {

            // get field value
            Object o = record.get(this.fieldNames[i], this.outputSchema);

            // Set field value in Flink tuple.
            // Partition columns are returned as String and
            //   need to be converted to original type.
            switch (this.outputSchema.get(i).getType()) {
                case INT:
                    if (o instanceof String) {
                        tuple.setField(Integer.parseInt((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case TINYINT:
                    if (o instanceof String) {
                        tuple.setField(Byte.parseByte((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case SMALLINT:
                    if (o instanceof String) {
                        tuple.setField(Short.parseShort((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case BIGINT:
                    if (o instanceof String) {
                        tuple.setField(Long.parseLong((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case BOOLEAN:
                    if (o instanceof String) {
                        tuple.setField(Boolean.parseBoolean((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case FLOAT:
                    if (o instanceof String) {
                        tuple.setField(Float.parseFloat((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case DOUBLE:
                    if (o instanceof String) {
                        tuple.setField(Double.parseDouble((String) o), i);
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case STRING:
                    tuple.setField(o, i);
                    break;
                case BINARY:
                    if (o instanceof String) {
                        throw new RuntimeException("Cannot handle partition keys of type BINARY.");
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case ARRAY:
                    if (o instanceof String) {
                        throw new RuntimeException("Cannot handle partition keys of type ARRAY.");
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case MAP:
                    if (o instanceof String) {
                        throw new RuntimeException("Cannot handle partition keys of type MAP.");
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                case STRUCT:
                    if (o instanceof String) {
                        throw new RuntimeException("Cannot handle partition keys of type STRUCT.");
                    } else {
                        tuple.setField(o, i);
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid Type");
            }
        }

        return (T) tuple;
    }
}
