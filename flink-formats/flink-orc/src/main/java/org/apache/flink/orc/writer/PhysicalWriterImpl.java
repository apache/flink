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

package org.apache.flink.orc.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataOutputStream;

import com.google.protobuf.CodedOutputStream;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.StreamName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.orc.impl.WriterImpl.getEstimatedBufferSize;

/**
 * A slightly customised clone of {@link org.apache.orc.impl.PhysicalFsWriter}.
 *
 * <p>Whereas PhysicalFsWriter implementation works on the basis of a Path, this implementation
 * leverages Flink's {@link FSDataOutputStream} to write the compressed data.
 *
 * <p>NOTE: If the ORC dependency version is updated, this file may have to be updated as well to be
 * in sync with the new version's PhysicalFsWriter.
 */
@Internal
public class PhysicalWriterImpl implements PhysicalWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalWriterImpl.class);
    private static final byte[] ZEROS = new byte[64 * 1024];
    private static final int HDFS_BUFFER_SIZE = 256 * 1024;

    protected final OutStream writer;
    private final CodedOutputStream protobufWriter;
    private final CompressionKind compress;
    private final Map<StreamName, BufferedStream> streams;
    private final HadoopShims shims;
    private final int maxPadding;
    private final int bufferSize;
    private final long blockSize;
    private final boolean addBlockPadding;
    private final boolean writeVariableLengthBlocks;

    private CompressionCodec codec;
    private FSDataOutputStream out;
    private long headerLength;
    private long stripeStart;
    private long blockOffset;
    private int metadataLength;
    private int footerLength;

    public PhysicalWriterImpl(FSDataOutputStream out, OrcFile.WriterOptions opts)
            throws IOException {
        if (opts.isEnforceBufferSize()) {
            this.bufferSize = opts.getBufferSize();
        } else {
            this.bufferSize =
                    getEstimatedBufferSize(
                            opts.getStripeSize(),
                            opts.getSchema().getMaximumId() + 1,
                            opts.getBufferSize());
        }

        this.out = out;
        this.blockOffset = 0;
        this.blockSize = opts.getBlockSize();
        this.maxPadding = (int) (opts.getPaddingTolerance() * (double) opts.getBufferSize());
        this.compress = opts.getCompress();
        this.codec = OrcCodecPool.getCodec(this.compress);
        this.streams = new TreeMap<>();
        this.writer =
                new OutStream("metadata", this.bufferSize, this.codec, new DirectStream(this.out));
        this.shims = opts.getHadoopShims();
        this.addBlockPadding = opts.getBlockPadding();
        this.protobufWriter = CodedOutputStream.newInstance(this.writer);
        this.writeVariableLengthBlocks = opts.getWriteVariableLengthBlocks();
    }

    @Override
    public void writeHeader() throws IOException {
        this.out.write("ORC".getBytes());
        this.headerLength = this.out.getPos();
    }

    @Override
    public OutputReceiver createDataStream(StreamName name) throws IOException {
        BufferedStream result = streams.get(name);

        if (result == null) {
            result = new BufferedStream();
            streams.put(name, result);
        }

        return result;
    }

    @Override
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index, CompressionCodec codec)
            throws IOException {
        OutputStream stream =
                new OutStream(this.toString(), bufferSize, codec, createDataStream(name));
        index.build().writeTo(stream);
        stream.flush();
    }

    @Override
    public void writeBloomFilter(
            StreamName name, OrcProto.BloomFilterIndex.Builder bloom, CompressionCodec codec)
            throws IOException {
        OutputStream stream =
                new OutStream(this.toString(), bufferSize, codec, createDataStream(name));
        bloom.build().writeTo(stream);
        stream.flush();
    }

    @Override
    public void finalizeStripe(
            OrcProto.StripeFooter.Builder footerBuilder,
            OrcProto.StripeInformation.Builder dirEntry)
            throws IOException {
        long indexSize = 0;
        long dataSize = 0;

        for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
            BufferedStream receiver = pair.getValue();
            if (!receiver.isSuppressed) {
                long streamSize = receiver.getOutputSize();
                StreamName name = pair.getKey();
                footerBuilder.addStreams(
                        OrcProto.Stream.newBuilder()
                                .setColumn(name.getColumn())
                                .setKind(name.getKind())
                                .setLength(streamSize));
                if (StreamName.Area.INDEX == name.getArea()) {
                    indexSize += streamSize;
                } else {
                    dataSize += streamSize;
                }
            }
        }

        dirEntry.setIndexLength(indexSize).setDataLength(dataSize);
        OrcProto.StripeFooter footer = footerBuilder.build();
        // Do we need to pad the file so the stripe doesn't straddle a block boundary?
        padStripe(indexSize + dataSize + footer.getSerializedSize());

        // write out the data streams
        for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
            pair.getValue().spillToDiskAndClear(out);
        }

        // Write out the footer.
        writeStripeFooter(footer, dataSize, indexSize, dirEntry);
    }

    @Override
    public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
        long startPosition = out.getPos();
        OrcProto.Metadata metadata = builder.build();
        writeMetadata(metadata);
        this.metadataLength = (int) (out.getPos() - startPosition);
    }

    @Override
    public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
        long bodyLength = out.getPos() - metadataLength;
        builder.setContentLength(bodyLength);
        builder.setHeaderLength(headerLength);
        long startPosition = out.getPos();
        OrcProto.Footer footer = builder.build();
        writeFileFooter(footer);
        this.footerLength = (int) (out.getPos() - startPosition);
    }

    @Override
    public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
        builder.setFooterLength(footerLength);
        builder.setMetadataLength(metadataLength);

        OrcProto.PostScript ps = builder.build();
        // need to write this uncompressed
        long startPosition = out.getPos();
        ps.writeTo(out);
        long length = out.getPos() - startPosition;

        if (length > 255) {
            throw new IllegalArgumentException("PostScript too large at " + length);
        }

        out.write((int) length);
        return out.getPos();
    }

    @Override
    public void close() {
        // Just release the codec but don't close the internal stream here to avoid
        // Stream Closed or ClosedChannelException when Flink performs checkpoint.
        OrcCodecPool.returnCodec(compress, codec);
        codec = null;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void appendRawStripe(ByteBuffer buffer, OrcProto.StripeInformation.Builder dirEntry)
            throws IOException {
        long start = out.getPos();
        int length = buffer.remaining();
        long availBlockSpace = blockSize - (start % blockSize);

        // see if stripe can fit in the current hdfs block, else pad the remaining
        // space in the block
        if (length < blockSize && length > availBlockSpace && addBlockPadding) {
            byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
            LOG.info(String.format("Padding ORC by %d bytes while merging..", availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0) {
                int writeLen = (int) Math.min(availBlockSpace, pad.length);
                out.write(pad, 0, writeLen);
                availBlockSpace -= writeLen;
            }
        }

        out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
        dirEntry.setOffset(start);
    }

    @Override
    public CompressionCodec getCompressionCodec() {
        return this.codec;
    }

    @Override
    public long getFileBytes(int column) {
        long size = 0;

        for (final Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
            final BufferedStream receiver = pair.getValue();
            if (!receiver.isSuppressed) {

                final StreamName name = pair.getKey();
                if (name.getColumn() == column && name.getArea() != StreamName.Area.INDEX) {
                    size += receiver.getOutputSize();
                }
            }
        }

        return size;
    }

    private void padStripe(long stripeSize) throws IOException {
        this.stripeStart = out.getPos();
        long previousBytesInBlock = (stripeStart - blockOffset) % blockSize;

        // We only have options if this isn't the first stripe in the block
        if (previousBytesInBlock > 0) {
            if (previousBytesInBlock + stripeSize >= blockSize) {
                // Try making a short block
                if (writeVariableLengthBlocks && shims.endVariableLengthBlock(out)) {
                    blockOffset = stripeStart;
                } else if (addBlockPadding) {
                    // if we cross the block boundary, figure out what we should do
                    long padding = blockSize - previousBytesInBlock;
                    if (padding <= maxPadding) {
                        writeZeros(out, padding);
                        stripeStart += padding;
                    }
                }
            }
        }
    }

    private void writeStripeFooter(
            OrcProto.StripeFooter footer,
            long dataSize,
            long indexSize,
            OrcProto.StripeInformation.Builder dirEntry)
            throws IOException {
        writeStripeFooter(footer);

        dirEntry.setOffset(stripeStart);
        dirEntry.setFooterLength(out.getPos() - stripeStart - dataSize - indexSize);
    }

    protected void writeMetadata(OrcProto.Metadata metadata) throws IOException {
        metadata.writeTo(protobufWriter);
        protobufWriter.flush();
        writer.flush();
    }

    protected void writeFileFooter(OrcProto.Footer footer) throws IOException {
        footer.writeTo(protobufWriter);
        protobufWriter.flush();
        writer.flush();
    }

    protected void writeStripeFooter(OrcProto.StripeFooter footer) throws IOException {
        footer.writeTo(protobufWriter);
        protobufWriter.flush();
        writer.flush();
    }

    private static void writeZeros(OutputStream output, long remaining) throws IOException {
        while (remaining > 0) {
            long size = Math.min(ZEROS.length, remaining);
            output.write(ZEROS, 0, (int) size);
            remaining -= size;
        }
    }

    private static class DirectStream implements OutputReceiver {
        private final FSDataOutputStream output;

        DirectStream(FSDataOutputStream output) {
            this.output = output;
        }

        public void output(ByteBuffer buffer) throws IOException {
            this.output.write(
                    buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }

        public void suppress() {
            throw new UnsupportedOperationException("Can't suppress direct stream");
        }
    }

    private static final class BufferedStream implements OutputReceiver {
        private boolean isSuppressed = false;
        private final List<ByteBuffer> output = new ArrayList<>();

        @Override
        public void output(ByteBuffer buffer) {
            if (!isSuppressed) {
                output.add(buffer);
            }
        }

        public void suppress() {
            isSuppressed = true;
            output.clear();
        }

        void spillToDiskAndClear(FSDataOutputStream raw) throws IOException {
            if (!isSuppressed) {
                for (ByteBuffer buffer : output) {
                    raw.write(
                            buffer.array(),
                            buffer.arrayOffset() + buffer.position(),
                            buffer.remaining());
                }
                output.clear();
            }
            isSuppressed = false;
        }

        public long getOutputSize() {
            long result = 0;
            for (ByteBuffer buffer : output) {
                result += buffer.remaining();
            }
            return result;
        }
    }
}
