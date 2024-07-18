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

/*
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * Interface of all file systems used by Flink. This interface may be extended to implement
 * distributed file systems, or local file systems. The abstraction by this file system is very
 * simple, and the set of available operations quite limited, to support the common denominator of a
 * wide range of file systems. For example, appending to or mutating existing files is not
 * supported.
 *
 * <p>Flink implements and supports some file system types directly (for example the default
 * machine-local file system). Other file system types are accessed by an implementation that
 * bridges to the suite of file systems supported by Hadoop (such as for example HDFS).
 *
 * <h2>Scope and Purpose</h2>
 *
 * <p>The purpose of this abstraction is used to expose a common and well defined interface for
 * access to files. This abstraction is used both by Flink's fault tolerance mechanism (storing
 * state and recovery data) and by reusable built-in connectors (file sources / sinks).
 *
 * <p>The purpose of this abstraction is <b>not</b> to give user programs an abstraction with
 * extreme flexibility and control across all possible file systems. That mission would be a folly,
 * as the differences in characteristics of even the most common file systems are already quite
 * large. It is expected that user programs that need specialized functionality of certain file
 * systems in their functions, operations, sources, or sinks instantiate the specialized file system
 * adapters directly.
 *
 * <h2>Data Persistence Contract</h2>
 *
 * <p>The FileSystem's {@link FSDataOutputStream output streams} are used to persistently store
 * data, both for results of streaming applications and for fault tolerance and recovery. It is
 * therefore crucial that the persistence semantics of these streams are well defined.
 *
 * <h3>Definition of Persistence Guarantees</h3>
 *
 * <p>Data written to an output stream is considered persistent, if two requirements are met:
 *
 * <ol>
 *   <li><b>Visibility Requirement:</b> It must be guaranteed that all other processes, machines,
 *       virtual machines, containers, etc. that are able to access the file see the data
 *       consistently when given the absolute file path. This requirement is similar to the
 *       <i>close-to-open</i> semantics defined by POSIX, but restricted to the file itself (by its
 *       absolute path).
 *   <li><b>Durability Requirement:</b> The file system's specific durability/persistence
 *       requirements must be met. These are specific to the particular file system. For example the
 *       {@link LocalFileSystem} does not provide any durability guarantees for crashes of both
 *       hardware and operating system, while replicated distributed file systems (like HDFS)
 *       typically guarantee durability in the presence of at most <i>n</i> concurrent node
 *       failures, where <i>n</i> is the replication factor.
 * </ol>
 *
 * <p>Updates to the file's parent directory (such that the file shows up when listing the directory
 * contents) are not required to be complete for the data in the file stream to be considered
 * persistent. This relaxation is important for file systems where updates to directory contents are
 * only eventually consistent.
 *
 * <p>The {@link FSDataOutputStream} has to guarantee data persistence for the written bytes once
 * the call to {@link FSDataOutputStream#close()} returns.
 *
 * <h3>Examples</h3>
 *
 * <h4>Fault-tolerant distributed file systems</h4>
 *
 * <p>For <b>fault-tolerant distributed file systems</b>, data is considered persistent once it has
 * been received and acknowledged by the file system, typically by having been replicated to a
 * quorum of machines (<i>durability requirement</i>). In addition the absolute file path must be
 * visible to all other machines that will potentially access the file (<i>visibility
 * requirement</i>).
 *
 * <p>Whether data has hit non-volatile storage on the storage nodes depends on the specific
 * guarantees of the particular file system.
 *
 * <p>The metadata updates to the file's parent directory are not required to have reached a
 * consistent state. It is permissible that some machines see the file when listing the parent
 * directory's contents while others do not, as long as access to the file by its absolute path is
 * possible on all nodes.
 *
 * <h4>Local file systems</h4>
 *
 * <p>A <b>local file system</b> must support the POSIX <i>close-to-open</i> semantics. Because the
 * local file system does not have any fault tolerance guarantees, no further requirements exist.
 *
 * <p>The above implies specifically that data may still be in the OS cache when considered
 * persistent from the local file system's perspective. Crashes that cause the OS cache to lose data
 * are considered fatal to the local machine and are not covered by the local file system's
 * guarantees as defined by Flink.
 *
 * <p>That means that computed results, checkpoints, and savepoints that are written only to the
 * local filesystem are not guaranteed to be recoverable from the local machine's failure, making
 * local file systems unsuitable for production setups.
 *
 * <h2>Updating File Contents</h2>
 *
 * <p>Many file systems either do not support overwriting contents of existing files at all, or do
 * not support consistent visibility of the updated contents in that case. For that reason, Flink's
 * FileSystem does not support appending to existing files, or seeking within output streams so that
 * previously written data could be overwritten.
 *
 * <h2>Overwriting Files</h2>
 *
 * <p>Overwriting files is in general possible. A file is overwritten by deleting it and creating a
 * new file. However, certain filesystems cannot make that change synchronously visible to all
 * parties that have access to the file. For example <a
 * href="https://aws.amazon.com/documentation/s3/">Amazon S3</a> guarantees only <i>eventual
 * consistency</i> in the visibility of the file replacement: Some machines may see the old file,
 * some machines may see the new file.
 *
 * <p>To avoid these consistency issues, the implementations of failure/recovery mechanisms in Flink
 * strictly avoid writing to the same file path more than once.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>Implementations of {@code FileSystem} must be thread-safe: The same instance of FileSystem is
 * frequently shared across multiple threads in Flink and must be able to concurrently create
 * input/output streams and list file metadata.
 *
 * <p>The {@link FSDataInputStream} and {@link FSDataOutputStream} implementations are strictly
 * <b>not thread-safe</b>. Instances of the streams should also not be passed between threads in
 * between read or write operations, because there are no guarantees about the visibility of
 * operations across threads (many operations do not create memory fences).
 *
 * <h2>Streams Safety Net</h2>
 *
 * <p>When application code obtains a FileSystem (via {@link FileSystem#get(URI)} or via {@link
 * Path#getFileSystem()}), the FileSystem instantiates a safety net for that FileSystem. The safety
 * net ensures that all streams created from the FileSystem are closed when the application task
 * finishes (or is canceled or failed). That way, the task's threads do not leak connections.
 *
 * <p>Internal runtime code can explicitly obtain a FileSystem that does not use the safety net via
 * {@link FileSystem#getUnguardedFileSystem(URI)}.
 *
 * @see FSDataInputStream
 * @see FSDataOutputStream
 */
@Experimental
public interface IFileSystem {

    /**
     * Returns the path of the file system's current working directory.
     *
     * @return the path of the file system's current working directory
     */
    Path getWorkingDirectory();

    /**
     * Returns the path of the user's home directory in this file system.
     *
     * @return the path of the user's home directory in this file system.
     */
    Path getHomeDirectory();

    /**
     * Returns a URI whose scheme and authority identify this file system.
     *
     * @return a URI whose scheme and authority identify this file system
     */
    URI getUri();

    /**
     * Return a file status object that represents the path.
     *
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist; IOException see specific
     *     implementation
     */
    FileStatus getFileStatus(Path f) throws IOException;

    /**
     * Return an array containing hostnames, offset and size of portions of the given file. For a
     * nonexistent file or regions, null will be returned. This call is most helpful with DFS, where
     * it returns hostnames of machines that contain the given file. The FileSystem will simply
     * return an elt containing 'localhost'.
     */
    BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException;

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    FSDataInputStream open(Path f, int bufferSize) throws IOException;

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f the file to open
     */
    FSDataInputStream open(Path f) throws IOException;

    /**
     * Creates a new {@link RecoverableWriter}. A recoverable writer creates streams that can
     * persist and recover their intermediate state. Persisting and recovering intermediate state is
     * a core building block for writing to files that span multiple checkpoints.
     *
     * <p>The returned object can act as a shared factory to open and recover multiple streams.
     *
     * <p>This method is optional on file systems and various file system implementations may not
     * support this method, throwing an {@code UnsupportedOperationException}.
     *
     * @return A RecoverableWriter for this file system.
     * @throws IOException Thrown, if the recoverable writer cannot be instantiated.
     */
    default RecoverableWriter createRecoverableWriter() throws IOException {
        throw new UnsupportedOperationException(
                "This file system does not support recoverable writers.");
    }

    /**
     * List the statuses of the files/directories in the given path if the path is a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given path
     * @throws IOException
     */
    FileStatus[] listStatus(Path f) throws IOException;

    /**
     * Check if exists.
     *
     * @param f source file
     */
    default boolean exists(final Path f) throws IOException {
        try {
            return (getFileStatus(f) != null);
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Tells if this {@link FileSystem} supports an optimised way to directly copy between given
     * paths. In other words if it implements {@link PathsCopyingFileSystem}.
     *
     * <p>At least one of, either source or destination belongs to this {@link IFileSystem}. One of
     * them can point to the local file system. In other words this request can correspond to
     * either: downloading a file from the remote file system, uploading a file to the remote file
     * system or duplicating a file in the remote file system.
     *
     * @param source The path of the source file to duplicate
     * @param destination The path where to duplicate the source file
     * @return true, if this {@link IFileSystem} can perform this operation more quickly compared to
     *     the generic code path of using streams.
     */
    default boolean canCopyPaths(Path source, Path destination) throws IOException {
        return false;
    }

    /**
     * Delete a file.
     *
     * @param f the path to delete
     * @param recursive if path is a directory and set to <code>true</code>, the directory is
     *     deleted else throws an exception. In case of a file the recursive can be set to either
     *     <code>true</code> or <code>false</code>
     * @return <code>true</code> if delete is successful, <code>false</code> otherwise
     * @throws IOException
     */
    boolean delete(Path f, boolean recursive) throws IOException;

    /**
     * Make the given file and all non-existent parents into directories. Has the semantics of Unix
     * 'mkdir -p'. Existence of the directory hierarchy is not an error.
     *
     * @param f the directory/directories to be created
     * @return <code>true</code> if at least one new directory has been created, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an I/O error occurs while creating the directory
     */
    boolean mkdirs(Path f) throws IOException;

    /**
     * Opens an FSDataOutputStream to a new file at the given path.
     *
     * <p>If the file already exists, the behavior depends on the given {@code WriteMode}. If the
     * mode is set to {@link FileSystem.WriteMode#NO_OVERWRITE}, then this method fails with an
     * exception.
     *
     * @param f The file path to write to
     * @param overwriteMode The action to take if a file or directory already exists at the given
     *     path.
     * @return The stream to the new file at the target path.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     */
    FSDataOutputStream create(Path f, FileSystem.WriteMode overwriteMode) throws IOException;

    /**
     * Renames the file/directory src to dst.
     *
     * @param src the file/directory to rename
     * @param dst the new name of the file/directory
     * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
     * @throws IOException
     */
    boolean rename(Path src, Path dst) throws IOException;

    /**
     * Returns true if this is a distributed file system. A distributed file system here means that
     * the file system is shared among all Flink processes that participate in a cluster or job and
     * that all these processes can see the same files.
     *
     * @return True, if this is a distributed file system, false otherwise.
     */
    boolean isDistributedFS();

    /**
     * Gets a description of the characteristics of this file system.
     *
     * @deprecated this method is not used anymore.
     */
    @Deprecated
    FileSystemKind getKind();

    /**
     * Initializes output directories on local file systems according to the given write mode.
     *
     * <ul>
     *   <li>WriteMode.NO_OVERWRITE &amp; parallel output:
     *       <ul>
     *         <li>A directory is created if the output path does not exist.
     *         <li>An existing directory is reused, files contained in the directory are NOT
     *             deleted.
     *         <li>An existing file raises an exception.
     *       </ul>
     *   <li>WriteMode.NO_OVERWRITE &amp; NONE parallel output:
     *       <ul>
     *         <li>An existing file or directory raises an exception.
     *       </ul>
     *   <li>WriteMode.OVERWRITE &amp; parallel output:
     *       <ul>
     *         <li>A directory is created if the output path does not exist.
     *         <li>An existing directory is reused, files contained in the directory are NOT
     *             deleted.
     *         <li>An existing file is deleted and replaced by a new directory.
     *       </ul>
     *   <li>WriteMode.OVERWRITE &amp; NONE parallel output:
     *       <ul>
     *         <li>An existing file or directory (and all its content) is deleted
     *       </ul>
     * </ul>
     *
     * <p>Files contained in an existing directory are not deleted, because multiple instances of a
     * DataSinkTask might call this function at the same time and hence might perform concurrent
     * delete operations on the file system (possibly deleting output files of concurrently running
     * tasks). Since concurrent DataSinkTasks are not aware of each other, coordination of delete
     * and create operations would be difficult.
     *
     * @param outPath Output path that should be prepared.
     * @param writeMode Write mode to consider.
     * @param createDirectory True, to initialize a directory at the given path, false to prepare
     *     space for a file.
     * @return True, if the path was successfully prepared, false otherwise.
     * @throws IOException Thrown, if any of the file system access operations failed.
     */
    boolean initOutPathLocalFS(
            Path outPath, FileSystem.WriteMode writeMode, boolean createDirectory)
            throws IOException;

    /**
     * Initializes output directories on distributed file systems according to the given write mode.
     *
     * <p>WriteMode.NO_OVERWRITE &amp; parallel output: - A directory is created if the output path
     * does not exist. - An existing file or directory raises an exception.
     *
     * <p>WriteMode.NO_OVERWRITE &amp; NONE parallel output: - An existing file or directory raises
     * an exception.
     *
     * <p>WriteMode.OVERWRITE &amp; parallel output: - A directory is created if the output path
     * does not exist. - An existing directory and its content is deleted and a new directory is
     * created. - An existing file is deleted and replaced by a new directory.
     *
     * <p>WriteMode.OVERWRITE &amp; NONE parallel output: - An existing file or directory is deleted
     * and replaced by a new directory.
     *
     * @param outPath Output path that should be prepared.
     * @param writeMode Write mode to consider.
     * @param createDirectory True, to initialize a directory at the given path, false otherwise.
     * @return True, if the path was successfully prepared, false otherwise.
     * @throws IOException Thrown, if any of the file system access operations failed.
     */
    boolean initOutPathDistFS(Path outPath, FileSystem.WriteMode writeMode, boolean createDirectory)
            throws IOException;
}
