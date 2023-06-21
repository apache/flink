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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.util.CollectionUtil;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;

/** Channel manager to manage the life cycle of spill channels. */
final class SpillChannelManager implements AutoCloseable {

    /** Collection of all currently open channels, to be closed and deleted during cleanup. */
    private final HashSet<FileIOChannel> openChannels;

    /** Collection of all temporary files created and to be removed when closing the sorter. */
    private final HashSet<FileIOChannel.ID> channelsToDeleteAtShutdown;

    private volatile boolean closed;

    public SpillChannelManager() {
        this.channelsToDeleteAtShutdown = CollectionUtil.newHashSetWithExpectedSize(64);
        this.openChannels = CollectionUtil.newHashSetWithExpectedSize(64);
    }

    /**
     * Adds a channel to the list of channels that are to be removed at shutdown.
     *
     * @param channel The channel id.
     */
    synchronized void registerChannelToBeRemovedAtShutdown(FileIOChannel.ID channel) {
        channelsToDeleteAtShutdown.add(channel);
    }

    /**
     * Removes a channel from the list of channels that are to be removed at shutdown.
     *
     * @param channel The channel id.
     */
    synchronized void unregisterChannelToBeRemovedAtShutdown(FileIOChannel.ID channel) {
        channelsToDeleteAtShutdown.remove(channel);
    }

    /**
     * Adds a channel reader/writer to the list of channels that are to be removed at shutdown.
     *
     * @param channel The channel reader/writer.
     */
    synchronized void registerOpenChannelToBeRemovedAtShutdown(FileIOChannel channel) {
        openChannels.add(channel);
    }

    /**
     * Removes a channel reader/writer from the list of channels that are to be removed at shutdown.
     *
     * @param channel The channel reader/writer.
     */
    synchronized void unregisterOpenChannelToBeRemovedAtShutdown(FileIOChannel channel) {
        openChannels.remove(channel);
    }

    @Override
    public synchronized void close() {

        if (this.closed) {
            return;
        }

        this.closed = true;

        for (Iterator<FileIOChannel> channels = this.openChannels.iterator();
                channels.hasNext(); ) {
            try {
                final FileIOChannel channel = channels.next();
                channels.remove();
                channel.closeAndDelete();
            } catch (Throwable ignored) {
            }
        }

        for (Iterator<FileIOChannel.ID> channels = this.channelsToDeleteAtShutdown.iterator();
                channels.hasNext(); ) {
            try {
                final FileIOChannel.ID channel = channels.next();
                channels.remove();
                final File f = new File(channel.getPath());
                if (f.exists()) {
                    f.delete();
                }
            } catch (Throwable ignored) {
            }
        }
    }
}
