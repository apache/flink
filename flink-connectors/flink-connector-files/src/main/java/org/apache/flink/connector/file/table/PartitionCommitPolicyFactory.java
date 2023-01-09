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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;

/** A factory to create {@link PartitionCommitPolicy} chain. */
@Internal
public class PartitionCommitPolicyFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String policyKind;
    private final String customClass;
    private final String successFileName;
    private final Configuration options;

    public PartitionCommitPolicyFactory(Configuration options) {
        this.policyKind = options.get(SINK_PARTITION_COMMIT_POLICY_KIND);
        this.customClass = options.get(SINK_PARTITION_COMMIT_POLICY_CLASS);
        this.successFileName = options.get(SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        this.options = options;
    }

    /** Create a policy chain. */
    public List<PartitionCommitPolicy> createPolicyChain(
            ClassLoader cl, Supplier<FileSystem> fsSupplier) {
        if (policyKind == null) {
            return Collections.emptyList();
        }
        String[] policyStrings = policyKind.split(",");
        return Arrays.stream(policyStrings)
                .map(
                        name -> {
                            switch (name.toLowerCase()) {
                                case PartitionCommitPolicy.METASTORE:
                                    return new MetastoreCommitPolicy();
                                case PartitionCommitPolicy.SUCCESS_FILE:
                                    return new SuccessFileCommitPolicy(
                                            successFileName, fsSupplier.get());
                                case PartitionCommitPolicy.CUSTOM:
                                    try {
                                        PartitionCommitPolicy policy =
                                                (PartitionCommitPolicy)
                                                        cl.loadClass(customClass).newInstance();
                                        policy.open(options);
                                        return policy;
                                    } catch (ClassNotFoundException
                                            | IllegalAccessException
                                            | InstantiationException e) {
                                        throw new RuntimeException(
                                                "Can not create new instance for custom class from "
                                                        + customClass,
                                                e);
                                    }
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unsupported policy: " + name);
                            }
                        })
                .collect(Collectors.toList());
    }
}
