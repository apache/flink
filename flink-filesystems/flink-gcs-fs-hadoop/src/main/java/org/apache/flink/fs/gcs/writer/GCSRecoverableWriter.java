package org.apache.flink.fs.gcs.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 *
 */
public class GCSRecoverableWriter implements RecoverableWriter {

	/**
	 * Opens a new recoverable stream to write to the given path.
	 * Whether existing files will be overwritten is implementation specific and should
	 * not be relied upon.
	 *
	 * @param path The path of the file/object to write to.
	 * @return A new RecoverableFsDataOutputStream writing a new file/object.
	 *
	 * @throws IOException Thrown if the stream could not be opened/initialized.
	 */
	@Override
	public RecoverableFsDataOutputStream open(Path path) throws IOException {
		return null;
	}

	/**
	 * Resumes a recoverable stream consistently at the point indicated by the given ResumeRecoverable.
	 * Future writes to the stream will continue / append the file as of that point.
	 *
	 * <p>This method is optional and whether it is supported is indicated through the
	 * {@link #supportsResume()} method.
	 *
	 * @param resumable The opaque handle with the recovery information.
	 * @return A recoverable stream writing to the file/object as it was at the point when the
	 *         ResumeRecoverable was created.
	 *
	 * @throws IOException Thrown, if resuming fails.
	 * @throws UnsupportedOperationException Thrown if this optional method is not supported.
	 */
	@Override
	public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException {
		return null;
	}

	/**
	 * Marks if the writer requires to do any additional cleanup/freeing of resources occupied
	 * as part of a {@link ResumeRecoverable}, e.g. temporarily files created or objects uploaded
	 * to external systems.
	 *
	 * <p>In case cleanup is required, then {@link #cleanupRecoverableState(ResumeRecoverable)} should
	 * be called.
	 *
	 * @return {@code true} if cleanup is required, {@code false} otherwise.
	 */
	@Override
	public boolean requiresCleanupOfRecoverableState() {
		return false;
	}

	/**
	 * Frees up any resources that were previously occupied in order to be able to
	 * recover from a (potential) failure. These can be temporary files that were written
	 * to the filesystem or objects that were uploaded to S3.
	 *
	 * <p><b>NOTE:</b> This operation should not throw an exception if the resumable has already
	 * been cleaned up and the resources have been freed. But the contract is that it will throw
	 * an {@link UnsupportedOperationException} if it is called for a {@code RecoverableWriter}
	 * whose {@link #requiresCleanupOfRecoverableState()} returns {@code false}.
	 *
	 * @param resumable The {@link ResumeRecoverable} whose state we want to clean-up.
	 * @return {@code true} if the resources were successfully freed, {@code false} otherwise
	 * (e.g. the file to be deleted was not there for any reason - already deleted or never created).
	 */
	@Override
	public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
		return false;
	}

	/**
	 * Recovers a recoverable stream consistently at the point indicated by the given CommitRecoverable
	 * for finalizing and committing. This will publish the target file with exactly the data
	 * that was written up to the point then the CommitRecoverable was created.
	 *
	 * @param resumable The opaque handle with the recovery information.
	 * @return A committer that publishes the target file.
	 *
	 * @throws IOException Thrown, if recovery fails.
	 */
	@Override
	public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable) throws IOException {
		return null;
	}

	/**
	 * The serializer for the CommitRecoverable types created in this writer.
	 * This serializer should be used to store the CommitRecoverable in checkpoint
	 * state or other forms of persistent state.
	 */
	@Override
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
		return null;
	}

	/**
	 * The serializer for the ResumeRecoverable types created in this writer.
	 * This serializer should be used to store the ResumeRecoverable in checkpoint
	 * state or other forms of persistent state.
	 */
	@Override
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
		return null;
	}

	/**
	 * Checks whether the writer and its streams support resuming (appending to) files after
	 * recovery (via the {@link #recover(ResumeRecoverable)} method).
	 *
	 * <p>If true, then this writer supports the {@link #recover(ResumeRecoverable)} method.
	 * If false, then that method may not be supported and streams can only be recovered via
	 * {@link #recoverForCommit(CommitRecoverable)}.
	 */
	@Override
	public boolean supportsResume() {
		return true;
	}
}
