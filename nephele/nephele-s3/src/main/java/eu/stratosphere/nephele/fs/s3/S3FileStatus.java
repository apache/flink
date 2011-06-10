package eu.stratosphere.nephele.fs.s3;

import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.Path;

public final class S3FileStatus implements FileStatus {

	private final Path path;
	
	private final long length;
	
	private final boolean isBucket;
	
	S3FileStatus(final Path path, final long length, final boolean isBucket) {
		this.path = path;
		this.length = length;
		this.isBucket = isBucket;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLen() {
		
		return this.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getBlockSize() {
		
		return this.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short getReplication() {
		
		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getModificationTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAccessTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isDir() {

		return this.isBucket;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getPath() {
		
		return this.path;
	}

}
