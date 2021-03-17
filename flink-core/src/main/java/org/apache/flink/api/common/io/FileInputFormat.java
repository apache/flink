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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.compression.Bzip2InputStreamFactory;
import org.apache.flink.api.common.io.compression.DeflateInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.GzipInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.XZInputStreamFactory;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class for {@link RichInputFormat}s that read from files. For specific input types the
 * {@link #nextRecord(Object)} and {@link #reachedEnd()} methods need to be implemented.
 * Additionally, one may override {@link #open(FileInputSplit)} and {@link #close()} to change the
 * life cycle behavior.
 *
 * <p>After the {@link #open(FileInputSplit)} method completed, the file input data is available
 * from the {@link #stream} field.
 */
@Public
public abstract class FileInputFormat<OT> extends RichInputFormat<OT, FileInputSplit> {

    // -------------------------------------- Constants -------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(FileInputFormat.class);

    private static final long serialVersionUID = 1L;

    /** The fraction that the last split may be larger than the others. */
    private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

    /** The timeout (in milliseconds) to wait for a filesystem stream to respond. */
    private static long DEFAULT_OPENING_TIMEOUT;

    /**
     * A mapping of file extensions to decompression algorithms based on DEFLATE. Such compressions
     * lead to unsplittable files.
     */
    protected static final Map<String, InflaterInputStreamFactory<?>>
            INFLATER_INPUT_STREAM_FACTORIES = new HashMap<String, InflaterInputStreamFactory<?>>();

    /** The splitLength is set to -1L for reading the whole split. */
    protected static final long READ_WHOLE_SPLIT_FLAG = -1L;

    static {
        initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
        initDefaultInflaterInputStreamFactories();
    }

    /**
     * Initialize defaults for input format. Needs to be a static method because it is configured
     * for local cluster execution.
     *
     * @param configuration The configuration to load defaults from
     */
    private static void initDefaultsFromConfiguration(Configuration configuration) {
        final long to =
                configuration.getLong(
                        ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
                        ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
        if (to < 0) {
            LOG.error(
                    "Invalid timeout value for filesystem stream opening: "
                            + to
                            + ". Using default value of "
                            + ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
            DEFAULT_OPENING_TIMEOUT = ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
        } else if (to == 0) {
            DEFAULT_OPENING_TIMEOUT = 300000; // 5 minutes
        } else {
            DEFAULT_OPENING_TIMEOUT = to;
        }
    }

    private static void initDefaultInflaterInputStreamFactories() {
        InflaterInputStreamFactory<?>[] defaultFactories = {
            DeflateInflaterInputStreamFactory.getInstance(),
            GzipInflaterInputStreamFactory.getInstance(),
            Bzip2InputStreamFactory.getInstance(),
            XZInputStreamFactory.getInstance(),
        };
        for (InflaterInputStreamFactory<?> inputStreamFactory : defaultFactories) {
            for (String fileExtension : inputStreamFactory.getCommonFileExtensions()) {
                registerInflaterInputStreamFactory(fileExtension, inputStreamFactory);
            }
        }
    }

    /**
     * Registers a decompression algorithm through a {@link
     * org.apache.flink.api.common.io.compression.InflaterInputStreamFactory} with a file extension
     * for transparent decompression.
     *
     * @param fileExtension of the compressed files
     * @param factory to create an {@link java.util.zip.InflaterInputStream} that handles the
     *     decompression format
     */
    public static void registerInflaterInputStreamFactory(
            String fileExtension, InflaterInputStreamFactory<?> factory) {
        synchronized (INFLATER_INPUT_STREAM_FACTORIES) {
            if (INFLATER_INPUT_STREAM_FACTORIES.put(fileExtension, factory) != null) {
                LOG.warn(
                        "Overwriting an existing decompression algorithm for \"{}\" files.",
                        fileExtension);
            }
        }
    }

    protected static InflaterInputStreamFactory<?> getInflaterInputStreamFactory(
            String fileExtension) {
        synchronized (INFLATER_INPUT_STREAM_FACTORIES) {
            return INFLATER_INPUT_STREAM_FACTORIES.get(fileExtension);
        }
    }

    /**
     * Returns the extension of a file name (!= a path).
     *
     * @return the extension of the file name or {@code null} if there is no extension.
     */
    protected static String extractFileExtension(String fileName) {
        checkNotNull(fileName);
        int lastPeriodIndex = fileName.lastIndexOf('.');
        if (lastPeriodIndex < 0) {
            return null;
        } else {
            return fileName.substring(lastPeriodIndex + 1);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Variables for internal operation.
    //  They are all transient, because we do not want them so be serialized
    // --------------------------------------------------------------------------------------------

    /** The input stream reading from the input file. */
    protected transient FSDataInputStream stream;

    /** The start of the split that this parallel instance must consume. */
    protected transient long splitStart;

    /** The length of the split that this parallel instance must consume. */
    protected transient long splitLength;

    /** The current split that this parallel instance must consume. */
    protected transient FileInputSplit currentSplit;

    // --------------------------------------------------------------------------------------------
    //  The configuration parameters. Configured on the instance and serialized to be shipped.
    // --------------------------------------------------------------------------------------------

    /**
     * The path to the file that contains the input.
     *
     * @deprecated Please override {@link FileInputFormat#supportsMultiPaths()} and use {@link
     *     FileInputFormat#getFilePaths()} and {@link FileInputFormat#setFilePaths(Path...)}.
     */
    @Deprecated protected Path filePath;

    /** The list of paths to files and directories that contain the input. */
    private Path[] filePaths;

    /** The minimal split size, set by the configure() method. */
    protected long minSplitSize = 0;

    /** The desired number of splits, as set by the configure() method. */
    protected int numSplits = -1;

    /** Stream opening timeout. */
    protected long openTimeout = DEFAULT_OPENING_TIMEOUT;

    /**
     * Some file input formats are not splittable on a block level (deflate) Therefore, the
     * FileInputFormat can only read whole files.
     */
    protected boolean unsplittable = false;

    /**
     * The flag to specify whether recursive traversal of the input directory structure is enabled.
     */
    protected boolean enumerateNestedFiles = false;

    /** Files filter for determining what files/directories should be included. */
    private FilePathFilter filesFilter = new GlobFilePathFilter();

    // --------------------------------------------------------------------------------------------
    //  Constructors
    // --------------------------------------------------------------------------------------------

    public FileInputFormat() {}

    protected FileInputFormat(Path filePath) {
        if (filePath != null) {
            setFilePath(filePath);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    /**
     * @return The path of the file to read.
     * @deprecated Please use getFilePaths() instead.
     */
    @Deprecated
    public Path getFilePath() {

        if (supportsMultiPaths()) {
            if (this.filePaths == null || this.filePaths.length == 0) {
                return null;
            } else if (this.filePaths.length == 1) {
                return this.filePaths[0];
            } else {
                throw new UnsupportedOperationException(
                        "FileInputFormat is configured with multiple paths. Use getFilePaths() instead.");
            }
        } else {
            return filePath;
        }
    }

    /**
     * Returns the paths of all files to be read by the FileInputFormat.
     *
     * @return The list of all paths to read.
     */
    public Path[] getFilePaths() {

        if (supportsMultiPaths()) {
            if (this.filePaths == null) {
                return new Path[0];
            }
            return this.filePaths;
        } else {
            if (this.filePath == null) {
                return new Path[0];
            }
            return new Path[] {filePath};
        }
    }

    public void setFilePath(String filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path cannot be null.");
        }

        // TODO The job-submission web interface passes empty args (and thus empty
        // paths) to compute the preview graph. The following is a workaround for
        // this situation and we should fix this.

        // comment (Stephan Ewen) this should be no longer relevant with the current Java/Scala
        // APIs.
        if (filePath.isEmpty()) {
            setFilePath(new Path());
            return;
        }

        try {
            this.setFilePath(new Path(filePath));
        } catch (RuntimeException rex) {
            throw new RuntimeException(
                    "Could not create a valid URI from the given file path name: "
                            + rex.getMessage());
        }
    }

    /**
     * Sets a single path of a file to be read.
     *
     * @param filePath The path of the file to read.
     */
    public void setFilePath(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path must not be null.");
        }

        setFilePaths(filePath);
    }

    /**
     * Sets multiple paths of files to be read.
     *
     * @param filePaths The paths of the files to read.
     */
    public void setFilePaths(String... filePaths) {
        Path[] paths = new Path[filePaths.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(filePaths[i]);
        }
        setFilePaths(paths);
    }

    /**
     * Sets multiple paths of files to be read.
     *
     * @param filePaths The paths of the files to read.
     */
    public void setFilePaths(Path... filePaths) {
        if (!supportsMultiPaths() && filePaths.length > 1) {
            throw new UnsupportedOperationException(
                    "Multiple paths are not supported by this FileInputFormat.");
        }
        if (filePaths.length < 1) {
            throw new IllegalArgumentException("At least one file path must be specified.");
        }
        if (filePaths.length == 1) {
            // set for backwards compatibility
            this.filePath = filePaths[0];
        } else {
            // clear file path in case it had been set before
            this.filePath = null;
        }

        this.filePaths = filePaths;
    }

    public long getMinSplitSize() {
        return minSplitSize;
    }

    public void setMinSplitSize(long minSplitSize) {
        if (minSplitSize < 0) {
            throw new IllegalArgumentException("The minimum split size cannot be negative.");
        }

        this.minSplitSize = minSplitSize;
    }

    public int getNumSplits() {
        return numSplits;
    }

    public void setNumSplits(int numSplits) {
        if (numSplits < -1 || numSplits == 0) {
            throw new IllegalArgumentException(
                    "The desired number of splits must be positive or -1 (= don't care).");
        }

        this.numSplits = numSplits;
    }

    public long getOpenTimeout() {
        return openTimeout;
    }

    public void setOpenTimeout(long openTimeout) {
        if (openTimeout < 0) {
            throw new IllegalArgumentException(
                    "The timeout for opening the input splits must be positive or zero (= infinite).");
        }
        this.openTimeout = openTimeout;
    }

    public void setNestedFileEnumeration(boolean enable) {
        this.enumerateNestedFiles = enable;
    }

    public boolean getNestedFileEnumeration() {
        return this.enumerateNestedFiles;
    }

    // --------------------------------------------------------------------------------------------
    // Getting information about the split that is currently open
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the start of the current split.
     *
     * @return The start of the split.
     */
    public long getSplitStart() {
        return splitStart;
    }

    /**
     * Gets the length or remaining length of the current split.
     *
     * @return The length or remaining length of the current split.
     */
    public long getSplitLength() {
        return splitLength;
    }

    public void setFilesFilter(FilePathFilter filesFilter) {
        this.filesFilter =
                Preconditions.checkNotNull(filesFilter, "Files filter should not be null");
    }

    // --------------------------------------------------------------------------------------------
    //  Pre-flight: Configuration, Splits, Sampling
    // --------------------------------------------------------------------------------------------

    /**
     * Configures the file input format by reading the file path from the configuration.
     *
     * @see
     *     org.apache.flink.api.common.io.InputFormat#configure(org.apache.flink.configuration.Configuration)
     */
    @Override
    public void configure(Configuration parameters) {

        if (getFilePaths().length == 0) {
            // file path was not specified yet. Try to set it from the parameters.
            String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
            if (filePath == null) {
                throw new IllegalArgumentException(
                        "File path was not specified in input format or configuration.");
            } else {
                setFilePath(filePath);
            }
        }

        if (!this.enumerateNestedFiles) {
            this.enumerateNestedFiles = parameters.getBoolean(ENUMERATE_NESTED_FILES_FLAG, false);
        }
    }

    /**
     * Obtains basic file statistics containing only file size. If the input is a directory, then
     * the size is the sum of all contained files.
     *
     * @see
     *     org.apache.flink.api.common.io.InputFormat#getStatistics(org.apache.flink.api.common.io.statistics.BaseStatistics)
     */
    @Override
    public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {

        final FileBaseStatistics cachedFileStats =
                cachedStats instanceof FileBaseStatistics ? (FileBaseStatistics) cachedStats : null;

        try {
            return getFileStats(
                    cachedFileStats, getFilePaths(), new ArrayList<>(getFilePaths().length));
        } catch (IOException ioex) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        "Could not determine statistics for paths '"
                                + Arrays.toString(getFilePaths())
                                + "' due to an io error: "
                                + ioex.getMessage());
            }
        } catch (Throwable t) {
            if (LOG.isErrorEnabled()) {
                LOG.error(
                        "Unexpected problem while getting the file statistics for paths '"
                                + Arrays.toString(getFilePaths())
                                + "': "
                                + t.getMessage(),
                        t);
            }
        }

        // no statistics available
        return null;
    }

    protected FileBaseStatistics getFileStats(
            FileBaseStatistics cachedStats, Path[] filePaths, ArrayList<FileStatus> files)
            throws IOException {

        long totalLength = 0;
        long latestModTime = 0;

        for (Path path : filePaths) {
            final FileSystem fs = FileSystem.get(path.toUri());
            final FileBaseStatistics stats = getFileStats(cachedStats, path, fs, files);

            if (stats.getTotalInputSize() == BaseStatistics.SIZE_UNKNOWN) {
                totalLength = BaseStatistics.SIZE_UNKNOWN;
            } else if (totalLength != BaseStatistics.SIZE_UNKNOWN) {
                totalLength += stats.getTotalInputSize();
            }
            latestModTime = Math.max(latestModTime, stats.getLastModificationTime());
        }

        // check whether the cached statistics are still valid, if we have any
        if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
            return cachedStats;
        }

        return new FileBaseStatistics(
                latestModTime, totalLength, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
    }

    protected FileBaseStatistics getFileStats(
            FileBaseStatistics cachedStats,
            Path filePath,
            FileSystem fs,
            ArrayList<FileStatus> files)
            throws IOException {

        // get the file info and check whether the cached statistics are still valid.
        final FileStatus file = fs.getFileStatus(filePath);
        long totalLength = 0;

        // enumerate all files
        if (file.isDir()) {
            totalLength += addFilesInDir(file.getPath(), files, false);
        } else {
            files.add(file);
            testForUnsplittable(file);
            totalLength += file.getLen();
        }

        // check the modification time stamp
        long latestModTime = 0;
        for (FileStatus f : files) {
            latestModTime = Math.max(f.getModificationTime(), latestModTime);
        }

        // check whether the cached statistics are still valid, if we have any
        if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
            return cachedStats;
        }

        // sanity check
        if (totalLength <= 0) {
            totalLength = BaseStatistics.SIZE_UNKNOWN;
        }
        return new FileBaseStatistics(
                latestModTime, totalLength, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
    }

    @Override
    public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] splits) {
        return new LocatableInputSplitAssigner(splits);
    }

    /**
     * Computes the input splits for the file. By default, one file block is one split. If more
     * splits are requested than blocks are available, then a split may be a fraction of a block and
     * splits may cross block boundaries.
     *
     * @param minNumSplits The minimum desired number of file splits.
     * @return The computed file splits.
     * @see org.apache.flink.api.common.io.InputFormat#createInputSplits(int)
     */
    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<>();
        long totalLength = 0;

        for (Path path : getFilePaths()) {
            final FileSystem fs = path.getFileSystem();
            final FileStatus pathFile = fs.getFileStatus(path);

            if (pathFile.isDir()) {
                totalLength += addFilesInDir(path, files, true);
            } else {
                testForUnsplittable(pathFile);

                files.add(pathFile);
                totalLength += pathFile.getLen();
            }
        }

        // returns if unsplittable
        if (unsplittable) {
            int splitNum = 0;
            for (final FileStatus file : files) {
                final FileSystem fs = file.getPath().getFileSystem();
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
                Set<String> hosts = new HashSet<String>();
                for (BlockLocation block : blocks) {
                    hosts.addAll(Arrays.asList(block.getHosts()));
                }
                long len = file.getLen();
                if (testForUnsplittable(file)) {
                    len = READ_WHOLE_SPLIT_FLAG;
                }
                FileInputSplit fis =
                        new FileInputSplit(
                                splitNum++,
                                file.getPath(),
                                0,
                                len,
                                hosts.toArray(new String[hosts.size()]));
                inputSplits.add(fis);
            }
            return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
        }

        final long maxSplitSize =
                totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

        // now that we have the files, generate the splits
        int splitNum = 0;
        for (final FileStatus file : files) {

            final FileSystem fs = file.getPath().getFileSystem();
            final long len = file.getLen();
            final long blockSize = file.getBlockSize();

            final long minSplitSize;
            if (this.minSplitSize <= blockSize) {
                minSplitSize = this.minSplitSize;
            } else {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(
                            "Minimal split size of "
                                    + this.minSplitSize
                                    + " is larger than the block size of "
                                    + blockSize
                                    + ". Decreasing minimal split size to block size.");
                }
                minSplitSize = blockSize;
            }

            final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
            final long halfSplit = splitSize >>> 1;

            final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

            if (len > 0) {

                // get the block locations and make sure they are in order with respect to their
                // offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
                Arrays.sort(blocks);

                long bytesUnassigned = len;
                long position = 0;

                int blockIndex = 0;

                while (bytesUnassigned > maxBytesForLastSplit) {
                    // get the block containing the majority of the data
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    // create a new split
                    FileInputSplit fis =
                            new FileInputSplit(
                                    splitNum++,
                                    file.getPath(),
                                    position,
                                    splitSize,
                                    blocks[blockIndex].getHosts());
                    inputSplits.add(fis);

                    // adjust the positions
                    position += splitSize;
                    bytesUnassigned -= splitSize;
                }

                // assign the last split
                if (bytesUnassigned > 0) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    final FileInputSplit fis =
                            new FileInputSplit(
                                    splitNum++,
                                    file.getPath(),
                                    position,
                                    bytesUnassigned,
                                    blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                }
            } else {
                // special case with a file of zero bytes size
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0) {
                    hosts = blocks[0].getHosts();
                } else {
                    hosts = new String[0];
                }
                final FileInputSplit fis =
                        new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    /**
     * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
     *
     * @return the total length of accepted files.
     */
    private long addFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
            throws IOException {
        final FileSystem fs = path.getFileSystem();

        long length = 0;

        for (FileStatus dir : fs.listStatus(path)) {
            if (dir.isDir()) {
                if (acceptFile(dir) && enumerateNestedFiles) {
                    length += addFilesInDir(dir.getPath(), files, logExcludedFiles);
                } else {
                    if (logExcludedFiles && LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Directory "
                                        + dir.getPath().toString()
                                        + " did not pass the file-filter and is excluded.");
                    }
                }
            } else {
                if (acceptFile(dir)) {
                    files.add(dir);
                    length += dir.getLen();
                    testForUnsplittable(dir);
                } else {
                    if (logExcludedFiles && LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Directory "
                                        + dir.getPath().toString()
                                        + " did not pass the file-filter and is excluded.");
                    }
                }
            }
        }
        return length;
    }

    protected boolean testForUnsplittable(FileStatus pathFile) {
        if (getInflaterInputStreamFactory(pathFile.getPath()) != null) {
            unsplittable = true;
            return true;
        }
        return false;
    }

    private InflaterInputStreamFactory<?> getInflaterInputStreamFactory(Path path) {
        String fileExtension = extractFileExtension(path.getName());
        if (fileExtension != null) {
            return getInflaterInputStreamFactory(fileExtension);
        } else {
            return null;
        }
    }

    /**
     * A simple hook to filter files and directories from the input. The method may be overridden.
     * Hadoop's FileInputFormat has a similar mechanism and applies the same filters by default.
     *
     * @param fileStatus The file status to check.
     * @return true, if the given file or directory is accepted
     */
    public boolean acceptFile(FileStatus fileStatus) {
        final String name = fileStatus.getPath().getName();
        return !name.startsWith("_")
                && !name.startsWith(".")
                && !filesFilter.filterPath(fileStatus.getPath());
    }

    /**
     * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file
     * described by the given offset.
     *
     * @param blocks The different blocks of the file. Must be ordered by their offset.
     * @param offset The offset of the position in the file.
     * @param startIndex The earliest index to look at.
     * @return The index of the block containing the given position.
     */
    private int getBlockIndexForPosition(
            BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();

            if (offset >= blockStart && offset < blockEnd) {
                // got the block where the split starts
                // check if the next block contains more than this one does
                if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
                    return i + 1;
                } else {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Opens an input stream to the file defined in the input format. The stream is positioned at
     * the beginning of the given split.
     *
     * <p>The stream is actually opened in an asynchronous thread to make sure any interruptions to
     * the thread working on the input format do not reach the file system.
     */
    @Override
    public void open(FileInputSplit fileSplit) throws IOException {

        this.currentSplit = fileSplit;
        this.splitStart = fileSplit.getStart();
        this.splitLength = fileSplit.getLength();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Opening input split "
                            + fileSplit.getPath()
                            + " ["
                            + this.splitStart
                            + ","
                            + this.splitLength
                            + "]");
        }

        // open the split in an asynchronous thread
        final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
        isot.start();

        try {
            this.stream = isot.waitForCompletion();
            this.stream = decorateInputStream(this.stream, fileSplit);
        } catch (Throwable t) {
            throw new IOException(
                    "Error opening the Input Split "
                            + fileSplit.getPath()
                            + " ["
                            + splitStart
                            + ","
                            + splitLength
                            + "]: "
                            + t.getMessage(),
                    t);
        }

        // get FSDataInputStream
        if (this.splitStart != 0) {
            this.stream.seek(this.splitStart);
        }
    }

    /**
     * This method allows to wrap/decorate the raw {@link FSDataInputStream} for a certain file
     * split, e.g., for decoding. When overriding this method, also consider adapting {@link
     * FileInputFormat#testForUnsplittable} if your stream decoration renders the input file
     * unsplittable. Also consider calling existing superclass implementations.
     *
     * @param inputStream is the input stream to decorated
     * @param fileSplit is the file split for which the input stream shall be decorated
     * @return the decorated input stream
     * @throws Throwable if the decoration fails
     * @see org.apache.flink.api.common.io.InputStreamFSInputWrapper
     */
    protected FSDataInputStream decorateInputStream(
            FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
        // Wrap stream in a extracting (decompressing) stream if file ends with a known compression
        // file extension.
        InflaterInputStreamFactory<?> inflaterInputStreamFactory =
                getInflaterInputStreamFactory(fileSplit.getPath());
        if (inflaterInputStreamFactory != null) {
            return new InputStreamFSInputWrapper(inflaterInputStreamFactory.create(stream));
        }

        return inputStream;
    }

    /** Closes the file input stream of the input format. */
    @Override
    public void close() throws IOException {
        if (this.stream != null) {
            // close input stream
            this.stream.close();
            stream = null;
        }
    }

    /**
     * Override this method to supports multiple paths. When this method will be removed, all
     * FileInputFormats have to support multiple paths.
     *
     * @return True if the FileInputFormat supports multiple paths, false otherwise.
     * @deprecated Will be removed for Flink 2.0.
     */
    @Deprecated
    public boolean supportsMultiPaths() {
        return false;
    }

    public String toString() {
        return getFilePaths() == null || getFilePaths().length == 0
                ? "File Input (unknown file)"
                : "File Input (" + Arrays.toString(this.getFilePaths()) + ')';
    }

    // ============================================================================================

    /**
     * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are the
     * size of the file and the average bytes of a single record. The statistics also have a
     * time-stamp that records the modification time of the file and indicates as such for which
     * time the statistics were valid.
     */
    public static class FileBaseStatistics implements BaseStatistics {

        protected final long fileModTime; // timestamp of the last modification

        protected final long fileSize; // size of the file(s) in bytes

        protected final float avgBytesPerRecord; // the average number of bytes for a record

        /**
         * Creates a new statistics object.
         *
         * @param fileModTime The timestamp of the latest modification of any of the involved files.
         * @param fileSize The size of the file, in bytes. <code>-1</code>, if unknown.
         * @param avgBytesPerRecord The average number of byte in a record, or <code>-1.0f</code>,
         *     if unknown.
         */
        public FileBaseStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
            this.fileModTime = fileModTime;
            this.fileSize = fileSize;
            this.avgBytesPerRecord = avgBytesPerRecord;
        }

        /**
         * Gets the timestamp of the last modification.
         *
         * @return The timestamp of the last modification.
         */
        public long getLastModificationTime() {
            return fileModTime;
        }

        /**
         * Gets the file size.
         *
         * @return The fileSize.
         * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getTotalInputSize()
         */
        @Override
        public long getTotalInputSize() {
            return this.fileSize;
        }

        /**
         * Gets the estimates number of records in the file, computed as the file size divided by
         * the average record width, rounded up.
         *
         * @return The estimated number of records in the file.
         * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getNumberOfRecords()
         */
        @Override
        public long getNumberOfRecords() {
            return (this.fileSize == SIZE_UNKNOWN
                            || this.avgBytesPerRecord == AVG_RECORD_BYTES_UNKNOWN)
                    ? NUM_RECORDS_UNKNOWN
                    : (long) Math.ceil(this.fileSize / this.avgBytesPerRecord);
        }

        /**
         * Gets the estimated average number of bytes per record.
         *
         * @return The average number of bytes per record.
         * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getAverageRecordWidth()
         */
        @Override
        public float getAverageRecordWidth() {
            return this.avgBytesPerRecord;
        }

        @Override
        public String toString() {
            return "size="
                    + this.fileSize
                    + ", recWidth="
                    + this.avgBytesPerRecord
                    + ", modAt="
                    + this.fileModTime;
        }
    }

    // ============================================================================================

    /**
     * Obtains a DataInputStream in an thread that is not interrupted. This is a necessary hack
     * around the problem that the HDFS client is very sensitive to InterruptedExceptions.
     */
    public static class InputSplitOpenThread extends Thread {

        private final FileInputSplit split;

        private final long timeout;

        private volatile FSDataInputStream fdis;

        private volatile Throwable error;

        private volatile boolean aborted;

        public InputSplitOpenThread(FileInputSplit split, long timeout) {
            super("Transient InputSplit Opener");
            setDaemon(true);

            this.split = split;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                final FileSystem fs = FileSystem.get(this.split.getPath().toUri());
                this.fdis = fs.open(this.split.getPath());

                // check for canceling and close the stream in that case, because no one will obtain
                // it
                if (this.aborted) {
                    final FSDataInputStream f = this.fdis;
                    this.fdis = null;
                    f.close();
                }
            } catch (Throwable t) {
                this.error = t;
            }
        }

        public FSDataInputStream waitForCompletion() throws Throwable {
            final long start = System.currentTimeMillis();
            long remaining = this.timeout;

            do {
                try {
                    // wait for the task completion
                    this.join(remaining);
                } catch (InterruptedException iex) {
                    // we were canceled, so abort the procedure
                    abortWait();
                    throw iex;
                }
            } while (this.error == null
                    && this.fdis == null
                    && (remaining = this.timeout + start - System.currentTimeMillis()) > 0);

            if (this.error != null) {
                throw this.error;
            }
            if (this.fdis != null) {
                return this.fdis;
            } else {
                // double-check that the stream has not been set by now. we don't know here whether
                // a) the opener thread recognized the canceling and closed the stream
                // b) the flag was set such that the stream did not see it and we have a valid
                // stream
                // In any case, close the stream and throw an exception.
                abortWait();

                final boolean stillAlive = this.isAlive();
                final StringBuilder bld = new StringBuilder(256);
                for (StackTraceElement e : this.getStackTrace()) {
                    bld.append("\tat ").append(e.toString()).append('\n');
                }
                throw new IOException(
                        "Input opening request timed out. Opener was "
                                + (stillAlive ? "" : "NOT ")
                                + " alive. Stack of split open thread:\n"
                                + bld.toString());
            }
        }

        /** Double checked procedure setting the abort flag and closing the stream. */
        private void abortWait() {
            this.aborted = true;
            final FSDataInputStream inStream = this.fdis;
            this.fdis = null;
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Throwable t) {
                }
            }
        }
    }

    // ============================================================================================
    //  Parameterization via configuration
    // ============================================================================================

    // ------------------------------------- Config Keys ------------------------------------------

    /** The config parameter which defines the input file path. */
    private static final String FILE_PARAMETER_KEY = "input.file.path";

    /** The config parameter which defines whether input directories are recursively traversed. */
    public static final String ENUMERATE_NESTED_FILES_FLAG = "recursive.file.enumeration";
}
