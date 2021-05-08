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

package org.apache.flink.addons.hbase1;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.hbase1.source.HBaseInputFormat;

/**
 * {@link InputFormat} subclass that wraps the access for HTables.
 *
 * @deprecated please use {@link org.apache.flink.connector.hbase1.source.HBaseInputFormat}.
 */
@Deprecated
public abstract class TableInputFormat<T extends Tuple> extends HBaseInputFormat<T> {
    private static final long serialVersionUID = 1L;

    public TableInputFormat(org.apache.hadoop.conf.Configuration hConf) {
        super(hConf);
    }
}
