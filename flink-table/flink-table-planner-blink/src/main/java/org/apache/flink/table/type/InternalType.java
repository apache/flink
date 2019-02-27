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

package org.apache.flink.table.type;

import java.io.Serializable;

/**
 * InternalType is the base type of all Flink SQL types and it is for internal computing
 * and code generator.
 *
 * <p>An InternalType may correspond to multiple data formats. The user uses the basic
 * Java data format, while we use a more efficient binary format inside Table for
 * performance, such as StringType corresponds to BinaryString internally, and JDK
 * String corresponds to the user layer.
 * So an Internal Type may correspond to multiple TypeSerializers.
 *
 * <p>We only support limited data formats, because all other data formats are converted
 * into data formats that we support internally when executed inside the table. Convert
 * it to the user when he needs it (For example, UDF and so on).
 */
public interface InternalType extends Serializable {
}
