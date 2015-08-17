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
package org.apache.flink.kafka_backport.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * This interface models the in-progress sending of data to a destination identified by an integer id.
 */
public interface Send {

    /**
     * The numeric id for the destination of this send
     */
    public String destination();

    /**
     * Is this send complete?
     */
    public boolean completed();

    /**
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     */
    public long writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * Size of the send
     */
    public long size();

}
