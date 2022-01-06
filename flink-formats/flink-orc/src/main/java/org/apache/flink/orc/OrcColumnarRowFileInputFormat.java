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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.ColumnBatchFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.util.List;

/** @deprecated Please use {@link OrcColumnarRowInputFormat}. */
@Deprecated
public class OrcColumnarRowFileInputFormat<BatchT, SplitT extends FileSourceSplit>
        extends OrcColumnarRowInputFormat<BatchT, SplitT> {

    public OrcColumnarRowFileInputFormat(
            OrcShim shim,
            Configuration hadoopConfig,
            TypeDescription schema,
            int[] selectedFields,
            List conjunctPredicates,
            int batchSize,
            ColumnBatchFactory batchFactory,
            TypeInformation producedTypeInfo) {
        super(
                shim,
                hadoopConfig,
                schema,
                selectedFields,
                conjunctPredicates,
                batchSize,
                batchFactory,
                producedTypeInfo);
    }
}
