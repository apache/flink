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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DatabindContext;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class that holds the necessary identifier fields that are used for JSON plan serialization
 * and deserialization. It is instantiated using {@link ExecNodeContext#newContext(Class)} when
 * creating a new instance of an {@link ExecNode}, so that is contains the info from the {@link
 * ExecNodeMetadata} annotation of the class with the latest {@link ExecNodeMetadata#version()}. It
 * can also be instantiated with {@link ExecNodeContext#ExecNodeContext(String)} automatically when
 * the {@link ExecNode} is deserialized from a JSON Plan, and in this case the {@link
 * ExecNodeContext} contains the version that is read from the JSON Plan and not the latest one. The
 * serialization format is {@code <name>_<version>}, see {@link ExecNodeContext#getTypeAsString()}.
 */
@Internal
public final class ExecNodeContext {

    private static final Pattern transformationNamePattern = Pattern.compile("[a-z\\-]+");

    /** This is used to assign a unique ID to every ExecNode. */
    private static final AtomicInteger idCounter = new AtomicInteger(0);

    /** Generate an unique ID for ExecNode. */
    public static int newNodeId() {
        return idCounter.incrementAndGet();
    }

    /** Reset the id counter to 0. */
    @VisibleForTesting
    public static void resetIdCounter() {
        idCounter.set(0);
    }

    private final Integer id;
    private final String name;
    private final Integer version;

    private ExecNodeContext() {
        this(null, null, null);
    }

    private ExecNodeContext(String name, Integer version) {
        this(null, name, version);
    }

    /**
     * @param id The unique id of the {@link ExecNode}. See {@link ExecNode#getId()}. It can be null
     *     initially and then later set by using {@link #withId(int)} which creates a new instance
     *     of {@link ExecNodeContext} since it's immutable. This way we can satisfy both the {@link
     *     ExecNodeBase#ExecNodeBase(int, ExecNodeContext, ReadableConfig, List, LogicalType,
     *     String)} ctor, which is used for the {@link JsonCreator} ctors, where the {@code id} and
     *     the {@code context} are read separately, and the {@link
     *     ExecNodeBase#getContextFromAnnotation()} which creates a new context with a new id
     *     provided by: {@link #newNodeId()}.
     * @param name The name of the {@link ExecNode}. See {@link ExecNodeMetadata#name()}.
     * @param version The version of the {@link ExecNode}. See {@link ExecNodeMetadata#version()}.
     */
    private ExecNodeContext(@Nullable Integer id, String name, Integer version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

    @JsonCreator
    public ExecNodeContext(String value) {
        this.id = null;
        String[] split = value.split("_");
        if ("null".equals(split[0]) || "null".equals(split[1])) {
            throw new TableException(String.format("Unsupported exec node type: '%s'.", value));
        }
        this.name = split[0];
        this.version = Integer.valueOf(split[1]);
    }

    /** The unique identifier for each ExecNode in the JSON plan. */
    int getId() {
        return checkNotNull(id);
    }

    /** The type identifying an ExecNode in the JSON plan. See {@link ExecNodeMetadata#name()}. */
    public String getName() {
        return name;
    }

    /** The version of the ExecNode in the JSON plan. See {@link ExecNodeMetadata#version()}. */
    public Integer getVersion() {
        return version;
    }

    /** Returns a new {@code uid} for transformations. */
    public String generateUid(String transformationName, ExecNodeConfig config) {
        if (!transformationNamePattern.matcher(transformationName).matches()) {
            throw new TableException(
                    "Invalid transformation name '"
                            + transformationName
                            + "'. "
                            + "This is a bug, please file an issue.");
        }
        final String uidPattern = config.get(ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT);
        // Note: name and version are not included in the UID by default as they would prevent
        // migration.
        // No version because: An operator can change its state layout and bump up the ExecNode
        // version, in this case the UID should still be able to map state even after plan
        // migration to the new version.
        // No name because: We might fuse operators in the future, and a new operator might
        // subscribe to multiple old UIDs.
        return StringUtils.replaceEach(
                uidPattern,
                new String[] {"<id>", "<type>", "<version>", "<transformation>"},
                new String[] {
                    String.valueOf(id), name, String.valueOf(version), transformationName
                });
    }

    /**
     * Set the unique ID of the node, so that the {@link ExecNodeContext}, together with the type
     * related {@link #name} and {@link #version}, stores all the necessary info to uniquely
     * reconstruct the {@link ExecNode}, and avoid storing the {@link #id} independently as a field
     * in {@link ExecNodeBase}.
     */
    public ExecNodeContext withId(int id) {
        return new ExecNodeContext(id, this.name, this.version);
    }

    /**
     * Returns the {@link #name} and {@link #version}, to be serialized into the JSON plan as one
     * string, which in turn will be parsed by {@link ExecNodeContext#ExecNodeContext(String)} when
     * deserialized from a JSON plan or when needed by {@link
     * ExecNodeTypeIdResolver#typeFromId(DatabindContext, String)}.
     */
    @JsonValue
    public String getTypeAsString() {
        if (name == null || version == null) {
            throw new TableException(
                    String.format(
                            "Can not serialize ExecNode with id: %d. Missing type, this is a bug,"
                                    + " please file a ticket.",
                            getId()));
        }
        return name + "_" + version;
    }

    @Override
    public String toString() {
        return getId() + "_" + getName() + "_" + getVersion();
    }

    public static <T extends ExecNode<?>> ExecNodeContext newContext(Class<T> execNodeClass) {
        ExecNodeMetadata metadata = ExecNodeMetadataUtil.latestAnnotation(execNodeClass);
        if (metadata == null) {
            if (!ExecNodeMetadataUtil.isUnsupported(execNodeClass)) {
                throw new IllegalStateException(
                        String.format(
                                "ExecNode: %s is not listed in the unsupported classes and"
                                        + " it is not annotated with: %s.",
                                execNodeClass.getCanonicalName(),
                                ExecNodeMetadata.class.getSimpleName()));
            }
            return new ExecNodeContext();
        }
        if (!ExecNodeMetadataUtil.execNodes().contains(execNodeClass)) {
            throw new IllegalStateException(
                    String.format(
                            "ExecNode: %s is not listed in the supported classes and yet is annotated with: %s.",
                            execNodeClass.getCanonicalName(),
                            ExecNodeMetadata.class.getSimpleName()));
        }
        return new ExecNodeContext(metadata.name(), metadata.version());
    }

    /**
     * Create a configuration for the {@link ExecNode}, ready to be persisted to a JSON plan.
     *
     * @param execNodeClass The {@link ExecNode} class.
     * @param tableConfig The planner configuration (include the {@link TableConfig}).
     * @return The {@link ExecNode} configuration, which contains the consumed options for the node,
     *     defined by {@link ExecNodeMetadata#consumedOptions()}, along with their values.
     */
    public static <T extends ExecNode<?>> ReadableConfig newPersistedConfig(
            Class<T> execNodeClass, ReadableConfig tableConfig) {
        return ExecNodeMetadataUtil.newPersistedConfig(
                execNodeClass,
                tableConfig,
                Stream.concat(
                        ExecNodeMetadataUtil.TABLE_CONFIG_OPTIONS.stream(),
                        ExecNodeMetadataUtil.EXECUTION_CONFIG_OPTIONS.stream()));
    }
}
