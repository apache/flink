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
import org.apache.flink.core.fs.FileSystem;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A factory to create {@link PartitionCommitPolicy} chain. */
@Internal
public class PartitionCommitPolicyFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String policyKind;
    private final String customClass;
    private final String successFileName;
    private final List<String> parameters;

    public PartitionCommitPolicyFactory(
            String policyKind,
            String customClass,
            String successFileName,
            List<String> parameters) {
        this.policyKind = policyKind;
        this.customClass = customClass;
        this.successFileName = successFileName;
        this.parameters = parameters;
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
                                        if (parameters != null && !parameters.isEmpty()) {
                                            String[] paramStrings =
                                                    parameters.toArray(new String[0]);
                                            Class<?>[] classes = new Class<?>[parameters.size()];
                                            for (int i = 0; i < parameters.size(); i++) {
                                                classes[i] = String.class;
                                            }
                                            return (PartitionCommitPolicy)
                                                    cl.loadClass(customClass)
                                                            .getConstructor(classes)
                                                            .newInstance((Object[]) paramStrings);
                                        } else {
                                            return (PartitionCommitPolicy)
                                                    cl.loadClass(customClass).newInstance();
                                        }
                                    } catch (ClassNotFoundException
                                            | IllegalAccessException
                                            | InstantiationException
                                            | NoSuchMethodException
                                            | InvocationTargetException e) {
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
