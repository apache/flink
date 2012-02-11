package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.util.StringUtils;

final class EnvelopeConsumptionLog {

	private static final Log LOG = LogFactory.getLog(EnvelopeConsumptionLog.class);

	private static final int LOG_WINDOW_SIZE = 8192;

	private static final int SIZE_OF_INTEGER = 4;

	private final File logFile;

	private final long numberOfInitialLogEntries;

	private final ByteBuffer outstandingEnvelopesAsByteBuffer;

	private final IntBuffer outstandingEnvelopesAsIntBuffer;

	private final ByteBuffer announcedEnvelopesAsByteBuffer;

	private final IntBuffer announcedEnvelopesAsIntBuffer;

	private final ExecutionVertexID vertexID;

	private final EnvelopeConsumptionTracker tracker;

	private long numberOfAnnouncedEnvelopes = 0L;

	private long numberOfEntriesReadFromLog = 0L;

	EnvelopeConsumptionLog(final ExecutionVertexID vertexID, final EnvelopeConsumptionTracker tracker) {

		this.vertexID = vertexID;
		this.tracker = tracker;

		// Check if there is a log file from a previous execution
		final String fileName = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH) + File.separator + "cl_" + this.vertexID;

		this.logFile = new File(fileName);

		if (this.logFile.exists()) {

			final long length = this.logFile.length();
			if (length % SIZE_OF_INTEGER != 0) {
				LOG.error("Channel consumption log " + fileName + " appears to be corrupt, discarding it...");
				this.logFile.delete();
				this.numberOfInitialLogEntries = 0L;
			} else {
				this.numberOfInitialLogEntries = length / SIZE_OF_INTEGER;
			}

			LOG.info("Found existing consumption log for task " + this.vertexID + " with a size of " + length
				+ " bytes");

		} else {
			this.numberOfInitialLogEntries = 0L;
		}

		this.outstandingEnvelopesAsByteBuffer = ByteBuffer.allocate(LOG_WINDOW_SIZE);
		this.outstandingEnvelopesAsIntBuffer = this.outstandingEnvelopesAsByteBuffer.asIntBuffer();

		this.announcedEnvelopesAsByteBuffer = ByteBuffer.allocate(LOG_WINDOW_SIZE);
		this.announcedEnvelopesAsIntBuffer = this.announcedEnvelopesAsByteBuffer.asIntBuffer();

		this.outstandingEnvelopesAsIntBuffer.limit(0);

		if (this.numberOfInitialLogEntries > 0) {
			loadNextOutstandingEnvelopes();
		}
	}

	void add(final int gateIndex, final int channelIndex) {

		if (this.outstandingEnvelopesAsIntBuffer.hasRemaining()) {
			addOutstandingEnvelope(gateIndex, channelIndex);
		} else {
			announce(gateIndex, channelIndex);
		}
	}

	void finish() {

		writeAnnouncedEnvelopesBufferToDisk();
	}

	private void addOutstandingEnvelope(final int gateIndex, final int channelIndex) {

		final int entryToTest = toEntry(gateIndex, channelIndex, false);

		boolean found = false;

		while (true) {

			for (int i = this.outstandingEnvelopesAsIntBuffer.position(); i < this.outstandingEnvelopesAsIntBuffer
				.limit(); ++i) {

				if (this.outstandingEnvelopesAsIntBuffer.get(i) == entryToTest) {
					// Mark data as available
					this.outstandingEnvelopesAsIntBuffer.put(i, setDataAvailability(entryToTest, true));
					found = true;
					break;
				}
			}

			if (!found) {

				if (this.outstandingEnvelopesAsIntBuffer.limit() == this.outstandingEnvelopesAsIntBuffer.capacity()) {
					loadNextOutstandingEnvelopes();
					continue;
				}

				final int newEntry = setDataAvailability(entryToTest, true);
				final int limit = this.outstandingEnvelopesAsIntBuffer.limit();
				this.outstandingEnvelopesAsIntBuffer.limit(limit + 1);
				this.outstandingEnvelopesAsIntBuffer.put(limit, newEntry);
			}

			break;
		}

		int newPosition = this.outstandingEnvelopesAsIntBuffer.position();
		int count = 0;
		for (int i = this.outstandingEnvelopesAsIntBuffer.position(); i < this.outstandingEnvelopesAsIntBuffer.limit(); ++i) {

			final int entry = this.outstandingEnvelopesAsIntBuffer.get(i);
			if (getDataAvailability(entry)) {
				announce(getInputGate(entry), getInputChannel(entry));
				newPosition = i + 1;
				++count;
			} else {
				break;
			}
		}

		this.outstandingEnvelopesAsIntBuffer.position(Math.min(this.outstandingEnvelopesAsIntBuffer.limit(),
			newPosition));

		if (count > 0) {
			System.out.println("Announced " + count + " buffers from log");
			System.out.println("Initial log entries: " + this.numberOfInitialLogEntries + ", announced "
				+ this.numberOfAnnouncedEnvelopes);
			System.out.println("Outstanding buffer: " + this.outstandingEnvelopesAsIntBuffer.remaining());
			showOustandingEnvelopeLog();
		}

		if (!this.outstandingEnvelopesAsIntBuffer.hasRemaining()) {
			loadNextOutstandingEnvelopes();
		}
	}

	private void showOustandingEnvelopeLog() {

		int dataAvailableCounter = 0;

		final int pos = this.outstandingEnvelopesAsIntBuffer.position();
		final int limit = this.outstandingEnvelopesAsIntBuffer.limit();

		for (int i = 0; i < this.outstandingEnvelopesAsIntBuffer.capacity(); ++i) {

			if (i < pos) {
				System.out.print('_');
				continue;
			}

			if (i >= limit) {
				System.out.print('_');
				continue;
			}

			final int entry = this.outstandingEnvelopesAsIntBuffer.get(i);

			final int channelIndex = getInputChannel(entry);
			final boolean dataAvailable = getDataAvailability(entry);
			if (dataAvailable) {
				++dataAvailableCounter;
			}

			char ch = (char) (((int) 'A') + channelIndex + (dataAvailable ? 0 : 32));

			System.out.print(ch);

		}

		System.out.println("");
		System.out.println("Data available: " + dataAvailableCounter);
	}

	private void loadNextOutstandingEnvelopes() {

		final int pos = this.outstandingEnvelopesAsIntBuffer.position();

		if (pos > 0) {

			final int rem = this.outstandingEnvelopesAsIntBuffer.remaining();

			for (int i = 0; i < rem; ++i) {
				this.outstandingEnvelopesAsIntBuffer.put(i, this.outstandingEnvelopesAsIntBuffer.get(i + pos));
			}

			this.outstandingEnvelopesAsIntBuffer.position(0);
			this.outstandingEnvelopesAsIntBuffer.limit(rem);
		}

		if (this.numberOfEntriesReadFromLog == this.numberOfInitialLogEntries) {
			return;
		}

		FileChannel fc = null;

		try {

			this.outstandingEnvelopesAsByteBuffer.position(this.outstandingEnvelopesAsIntBuffer.limit()
				* SIZE_OF_INTEGER);
			this.outstandingEnvelopesAsByteBuffer.limit(this.outstandingEnvelopesAsByteBuffer.capacity());

			fc = new FileInputStream(this.logFile).getChannel();
			fc.position(this.numberOfEntriesReadFromLog * SIZE_OF_INTEGER);

			int totalBytesRead = 0;

			while (this.outstandingEnvelopesAsByteBuffer.hasRemaining()) {

				final int bytesRead = fc.read(this.outstandingEnvelopesAsByteBuffer);
				if (bytesRead == -1) {
					break;
				}

				totalBytesRead += bytesRead;
			}

			if (totalBytesRead % SIZE_OF_INTEGER != 0) {
				LOG.error("Read " + totalBytesRead + " from " + this.logFile.getAbsolutePath()
					+ ", file may be corrupt");
			}

			final int numberOfNewEntries = totalBytesRead / SIZE_OF_INTEGER;

			this.outstandingEnvelopesAsIntBuffer.limit(this.outstandingEnvelopesAsIntBuffer.limit()
				+ numberOfNewEntries);

			this.numberOfEntriesReadFromLog += numberOfNewEntries;

			fc.close();

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		} finally {

			if (fc != null) {
				try {
					fc.close();
				} catch (IOException ioe) {
				}
			}
		}
	}

	private void writeAnnouncedEnvelopesBufferToDisk() {

		FileChannel fc = null;

		try {

			this.announcedEnvelopesAsIntBuffer.flip();
			this.announcedEnvelopesAsByteBuffer.position(this.announcedEnvelopesAsIntBuffer.position()
				* SIZE_OF_INTEGER);
			this.announcedEnvelopesAsByteBuffer.limit(this.announcedEnvelopesAsIntBuffer.limit() * SIZE_OF_INTEGER);

			fc = new FileOutputStream(this.logFile, true).getChannel();

			while (this.announcedEnvelopesAsByteBuffer.hasRemaining()) {
				fc.write(this.announcedEnvelopesAsByteBuffer);
			}

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		} finally {

			if (fc != null) {
				try {
					fc.close();
				} catch (IOException ioe) {
				}
			}

			this.announcedEnvelopesAsIntBuffer.clear();
			this.announcedEnvelopesAsByteBuffer.clear();
		}

	}

	private void announce(final int gateIndex, final int channelIndex) {

		this.tracker.announceData(gateIndex, channelIndex);

		if (++this.numberOfAnnouncedEnvelopes < this.numberOfInitialLogEntries) {
			return;
		}

		this.announcedEnvelopesAsIntBuffer.put(toEntry(gateIndex, channelIndex, false));

		if (!this.announcedEnvelopesAsIntBuffer.hasRemaining()) {
			writeAnnouncedEnvelopesBufferToDisk();
		}
	}

	private static int toEntry(final int gateIndex, final int channelIndex, final boolean dataAvailable) {

		int entry = 0;
		entry = setInputGate(entry, gateIndex);
		entry = setInputChannel(entry, channelIndex);
		entry = setDataAvailability(entry, dataAvailable);

		return entry;
	}

	private static int setInputGate(final int entry, final int gateIndex) {

		if ((gateIndex >>> 7) != 0) {
			throw new IllegalArgumentException("Gate index " + gateIndex + " cannot be stored in 7 bits");
		}

		return ((entry & 0xffffff01) | ((gateIndex & 0x7f) << 1));
	}

	private static int getInputGate(final int entry) {

		return ((entry >>> 1) & 0x7f);
	}

	private static int setInputChannel(final int entry, final int channelIndex) {

		if ((channelIndex >>> 24) != 0) {
			throw new IllegalArgumentException("Channel index " + channelIndex + " cannot be stored in 24 bits");
		}

		return ((entry & 0xff) | ((channelIndex & 0xffffff) << 8));
	}

	private static int getInputChannel(final int entry) {

		return ((entry >>> 8) & 0xffffff);
	}

	private static int setDataAvailability(final int entry, final boolean dataAvailable) {

		return ((entry & 0xfffffffe) | ((dataAvailable ? 1 : 0) & 0x01));
	}

	private static boolean getDataAvailability(final int entry) {

		return ((entry & 0x01) > 0);
	}
}
