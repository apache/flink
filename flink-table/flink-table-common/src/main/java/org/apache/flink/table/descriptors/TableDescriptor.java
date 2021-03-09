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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;

/**
 * Describes a table consisting of a connector (in a given update mode) and a format.
 *
 * @param <D> return type for builder pattern
 */
@PublicEvolving
public abstract class TableDescriptor<D extends TableDescriptor<D>> extends DescriptorBase {

    private final ConnectorDescriptor connectorDescriptor;

    private @Nullable FormatDescriptor formatDescriptor;

    private @Nullable String updateMode;

    protected TableDescriptor(ConnectorDescriptor connectorDescriptor) {
        this.connectorDescriptor =
                Preconditions.checkNotNull(connectorDescriptor, "Connector must not be null.");
    }

    /** Specifies the format that defines how to read data from a connector. */
    @SuppressWarnings("unchecked")
    public D withFormat(FormatDescriptor format) {
        formatDescriptor = Preconditions.checkNotNull(format, "Format must not be null.");
        return (D) this;
    }

    /**
     * Declares how to perform the conversion between a dynamic table and an external connector.
     *
     * <p>In append mode, a dynamic table and an external connector only exchange INSERT messages.
     *
     * @see #inRetractMode()
     * @see #inUpsertMode()
     */
    @SuppressWarnings("unchecked")
    public D inAppendMode() {
        updateMode = UPDATE_MODE_VALUE_APPEND;
        return (D) this;
    }

    /**
     * Declares how to perform the conversion between a dynamic table and an external connector.
     *
     * <p>In retract mode, a dynamic table and an external connector exchange ADD and RETRACT
     * messages.
     *
     * <p>An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and
     * an UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
     * the updating (new) row.
     *
     * <p>In this mode, a key must not be defined as opposed to upsert mode. However, every update
     * consists of two messages which is less efficient.
     *
     * @see #inAppendMode()
     * @see #inUpsertMode()
     */
    @SuppressWarnings("unchecked")
    public D inRetractMode() {
        updateMode = UPDATE_MODE_VALUE_RETRACT;
        return (D) this;
    }

    /**
     * Declares how to perform the conversion between a dynamic table and an external connector.
     *
     * <p>In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE
     * messages.
     *
     * <p>This mode requires a (possibly composite) unique key by which updates can be propagated.
     * The external connector needs to be aware of the unique key attribute in order to apply
     * messages correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes
     * as DELETE messages.
     *
     * <p>The main difference to a retract stream is that UPDATE changes are encoded with a single
     * message and are therefore more efficient.
     *
     * @see #inAppendMode()
     * @see #inRetractMode()
     */
    @SuppressWarnings("unchecked")
    public D inUpsertMode() {
        updateMode = UPDATE_MODE_VALUE_UPSERT;
        return (D) this;
    }

    /** Converts this descriptor into a set of properties. */
    @Override
    public final Map<String, String> toProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        // this performs only basic validation
        // more validation can only happen within a factory
        if (connectorDescriptor.isFormatNeeded() && formatDescriptor == null) {
            throw new ValidationException(
                    String.format(
                            "The connector %s requires a format description.",
                            connectorDescriptor.getClass().getName()));
        } else if (!connectorDescriptor.isFormatNeeded() && formatDescriptor != null) {
            throw new ValidationException(
                    String.format(
                            "The connector %s does not require a format description but %s found.",
                            connectorDescriptor.getClass().getName(),
                            formatDescriptor.getClass().getName()));
        }

        properties.putProperties(connectorDescriptor.toProperties());

        if (formatDescriptor != null) {
            properties.putProperties(formatDescriptor.toProperties());
        }

        if (updateMode != null) {
            properties.putString(UPDATE_MODE, updateMode);
        }

        properties.putProperties(additionalProperties());

        return properties.asMap();
    }

    /** Enables adding more specific properties to {@link #toProperties()}. */
    protected Map<String, String> additionalProperties() {
        return Collections.emptyMap();
    }
}
