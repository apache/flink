package eu.stratosphere.pact.common.io.statistics;

/**
 * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are the size of the file
 * and the average bytes of a single record. The statistics also have a time-stamp that records the modification
 * time of the file and indicates as such for which time the statistics were valid.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FileBaseStatistics implements BaseStatistics {
	
	private long fileModTime; // timestamp of the last modification

	private long fileSize; // size of the file(s) in bytes

	private float avgBytesPerRecord; // the average number of bytes for a record

	/**
	 * Creates a new statistics object.
	 * 
	 * @param fileModTime
	 *        The timestamp of the latest modification of any of the involved files.
	 * @param fileSize
	 *        The size of the file, in bytes. <code>-1</code>, if unknown.
	 * @param avgBytesPerRecord
	 *        The average number of byte in a record, or <code>-1.0f</code>, if unknown.
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
	 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getTotalInputSize()
	 */
	@Override
	public long getTotalInputSize()
	{
		return this.fileSize;
	}

	/**
	 * Gets the estimates number of records in the file, computed as the file size divided by the
	 * average record width, rounded up.
	 * 
	 * @return The estimated number of records in the file.
	 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getNumberOfRecords()
	 */
	@Override
	public long getNumberOfRecords()
	{
		return (long) Math.ceil(this.fileSize / this.avgBytesPerRecord);
	}

	/**
	 * Gets the estimated average number of bytes per record.
	 * 
	 * @return The average number of bytes per record.
	 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getAverageRecordWidth()
	 */
	@Override
	public float getAverageRecordWidth()
	{
		return this.avgBytesPerRecord;
	}
	
	/**
	 * Sets the timestamp of the last modification. 
	 * 
	 * @param fileModTime the timestamp of the last modification.
	 */
	public void setFileModTime(long fileModTime) {
		this.fileModTime = fileModTime;
	}

	/**
	 * Sets the file size.
	 * 
	 * @param fileSize the file size.
	 */
	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	/**
	 * Sets the estimated average number of bytes per record.
	 * 
	 * @param avgBytesPerRecord the estimated average number of bytes per record.
	 */
	public void setAvgBytesPerRecord(float avgBytesPerRecord) {
		this.avgBytesPerRecord = avgBytesPerRecord;
	}
}