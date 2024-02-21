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

package org.apache.flink.examples.java.util;

/** The info about the deprecation of DataSet API. */
public class DataSetDeprecationInfo {

    public static final String DATASET_DEPRECATION_INFO =
            "All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future"
                    + " Flink major version. You can still build your application in DataSet, but you should move to"
                    + " either the DataStream and/or Table API. This class is retained for testing purposes."
                    + " See Also: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741";
}
