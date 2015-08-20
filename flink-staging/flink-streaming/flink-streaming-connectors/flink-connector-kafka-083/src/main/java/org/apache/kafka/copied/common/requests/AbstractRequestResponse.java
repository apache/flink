/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common.requests;

import org.apache.kafka.copied.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequestResponse {
    protected final Struct struct;


    public AbstractRequestResponse(Struct struct) {
        this.struct = struct;
    }

    public Struct toStruct() {
        return struct;
    }

    /**
     * Get the serialized size of this object
     */
    public int sizeOf() {
        return struct.sizeOf();
    }

    /**
     * Write this object to a buffer
     */
    public void writeTo(ByteBuffer buffer) {
        struct.writeTo(buffer);
    }

    @Override
    public String toString() {
        return struct.toString();
    }

    @Override
    public int hashCode() {
        return struct.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractRequestResponse other = (AbstractRequestResponse) obj;
        return struct.equals(other.struct);
    }
}
