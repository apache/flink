/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sorter for buffered input in the form of Key-Value Style. First, sort and spill buffered inputs
 * (without data copy, just write index and normalized key). Second, merge disk outputs and return
 * iterator.
 *
 * <p>For Hash Aggregation：We store the data in MemorySegmentHashTable in KeyValue format. When
 * memory is not enough, we spill all the data in memory onto disk and degenerate it into Sort
 * Aggregation. So we need a BufferedKVExternalSorter to write the data that already in memory to
 * disk, and then carry out SortMerge.
 */
public class BufferedKVExternalSorter {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedKVExternalSorter.class);

    private volatile boolean closed = false;

    private final NormalizedKeyComputer nKeyComputer;
    private final RecordComparator comparator;
    private final BinaryRowDataSerializer keySerializer;
    private final BinaryRowDataSerializer valueSerializer;
    private final IndexedSorter sorter;

    private final BinaryKVExternalMerger merger;

    private final IOManager ioManager;
    private final int maxNumFileHandles;
    private final FileIOChannel.Enumerator enumerator;
    private final List<ChannelWithMeta> channelIDs = new ArrayList<>();
    private final SpillChannelManager channelManager;

    private int pageSize;

    // metric
    private long numSpillFiles;
    private long spillInBytes;
    private long spillInCompressedBytes;

    private final boolean compressionEnable;
    private final BlockCompressionFactory compressionCodecFactory;
    private final int compressionBlockSize;

    public BufferedKVExternalSorter(
            IOManager ioManager,
            BinaryRowDataSerializer keySerializer,
            BinaryRowDataSerializer valueSerializer,
            NormalizedKeyComputer nKeyComputer,
            RecordComparator comparator,
            int pageSize,
            Configuration conf)
            throws IOException {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.nKeyComputer = nKeyComputer;
        this.comparator = comparator;
        this.pageSize = pageSize;
        this.sorter = new QuickSort();
        this.maxNumFileHandles =
                conf.getInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES);
        this.compressionEnable =
                conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
        this.compressionCodecFactory =
                this.compressionEnable
                        ? BlockCompressionFactory.createBlockCompressionFactory(
                                BlockCompressionFactory.CompressionFactoryName.LZ4.toString())
                        : null;
        this.compressionBlockSize =
                (int)
                        MemorySize.parse(
                                        conf.getString(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE))
                                .getBytes();
        this.ioManager = ioManager;
        this.enumerator = this.ioManager.createChannelEnumerator();
        this.channelManager = new SpillChannelManager();
        this.merger =
                new BinaryKVExternalMerger(
                        ioManager,
                        pageSize,
                        maxNumFileHandles,
                        channelManager,
                        keySerializer,
                        valueSerializer,
                        comparator,
                        compressionEnable,
                        compressionCodecFactory,
                        compressionBlockSize);
    }

    public MutableObjectIterator<Tuple2<BinaryRowData, BinaryRowData>> getKVIterator()
            throws IOException {
        // 1. merge if more than maxNumFile
        // merge channels until sufficient file handles are available
        List<ChannelWithMeta> channelIDs = this.channelIDs;
        while (!closed && channelIDs.size() > this.maxNumFileHandles) {
            channelIDs = merger.mergeChannelList(channelIDs);
        }

        // 2. final merge
        List<FileIOChannel> openChannels = new ArrayList<>();
        BinaryMergeIterator<Tuple2<BinaryRowData, BinaryRowData>> iterator =
                merger.getMergingIterator(channelIDs, openChannels);
        channelManager.addOpenChannels(openChannels);

        return iterator;
    }

    public void sortAndSpill(
            ArrayList<MemorySegment> recordBufferSegments, long numElements, MemorySegmentPool pool)
            throws IOException {

        // 1. sort buffer
        BinaryKVInMemorySortBuffer buffer =
                BinaryKVInMemorySortBuffer.createBuffer(
                        nKeyComputer,
                        keySerializer,
                        valueSerializer,
                        comparator,
                        recordBufferSegments,
                        numElements,
                        pool);
        this.sorter.sort(buffer);

        // 2. spill
        FileIOChannel.ID channel = enumerator.next();
        channelManager.addChannel(channel);

        AbstractChannelWriterOutputView output = null;
        int bytesInLastBuffer;
        int blockCount;
        try {
            numSpillFiles++;
            output =
                    FileChannelUtil.createOutputView(
                            ioManager,
                            channel,
                            compressionEnable,
                            compressionCodecFactory,
                            compressionBlockSize,
                            pageSize);
            buffer.writeToOutput(output);
            spillInBytes += output.getNumBytes();
            spillInCompressedBytes += output.getNumCompressedBytes();
            bytesInLastBuffer = output.close();
            blockCount = output.getBlockCount();
            LOG.info(
                    "here spill the {}th kv external buffer data with {} bytes and {} compressed bytes",
                    numSpillFiles,
                    spillInBytes,
                    spillInCompressedBytes);
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        }
        channelIDs.add(new ChannelWithMeta(channel, blockCount, bytesInLastBuffer));
    }

    public void close() {
        if (closed) {
            return;
        }
        // mark as closed
        closed = true;
        merger.close();
        channelManager.close();
    }
}
