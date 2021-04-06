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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

/** Utility routines to process {@link ByteBuf}. */
public class ByteBufUtils {

    /**
     * Accumulates data from <tt>source</tt> to <tt>target</tt>. If no data has been accumulated yet
     * and <tt>source</tt> has enough data, <tt>source</tt> will be returned directly. Otherwise,
     * data will be copied into <tt>target</tt>. If the size of data copied after this operation has
     * reached <tt>targetAccumulationSize</tt>, <tt>target</tt> will be returned, otherwise
     * <tt>null</tt> will be returned to indicate more data is required.
     *
     * @param target The target buffer.
     * @param source The source buffer.
     * @param targetAccumulationSize The target size of data to accumulate.
     * @param accumulatedSize The size of data accumulated so far.
     * @return The ByteBuf containing accumulated data. If not enough data has been accumulated,
     *     <tt>null</tt> will be returned.
     */
    @Nullable
    public static ByteBuf accumulate(
            ByteBuf target, ByteBuf source, int targetAccumulationSize, int accumulatedSize) {
        if (accumulatedSize == 0 && source.readableBytes() >= targetAccumulationSize) {
            return source;
        }

        int copyLength = Math.min(source.readableBytes(), targetAccumulationSize - accumulatedSize);
        if (copyLength > 0) {
            target.writeBytes(source, copyLength);
        }

        if (accumulatedSize + copyLength == targetAccumulationSize) {
            return target;
        }

        return null;
    }
}
