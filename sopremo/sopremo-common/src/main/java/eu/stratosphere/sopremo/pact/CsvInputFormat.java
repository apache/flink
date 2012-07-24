package eu.stratosphere.sopremo.pact;

import static eu.stratosphere.sopremo.pact.IOConstants.COLUMN_NAMES;
import static eu.stratosphere.sopremo.pact.IOConstants.ENCODING;
import static eu.stratosphere.sopremo.pact.IOConstants.FIELD_DELIMITER;
import static eu.stratosphere.sopremo.pact.IOConstants.SCHEMA;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * InputFormat that interpretes the input data as a csv representation.
 */
public class CsvInputFormat extends FileInputFormat {
	/**
	 * @author Arvid Heise
	 */
	public static class CountingReader extends Reader {
		private long start = 0, relativePos = 0, limit = 0;

		private boolean reachedLimit = false;

		private ByteBuffer streamBuffer = ByteBuffer.allocate(100);

		private CharBuffer charBuffer = CharBuffer.allocate(100);

		private FSDataInputStream stream;

		protected CharsetDecoder decoder;

		public CountingReader(FSDataInputStream stream, Charset charset, long start, long limit) {
			this.stream = stream;
			this.decoder = charset.newDecoder();
			this.start = start;
			this.limit = limit;
			// mark as empty
			this.charBuffer.limit(0);
		}

		/**
		 * Returns the pos.
		 * 
		 * @return the pos
		 */
		public long getRelativePos() {
			return this.relativePos;
		}

		public boolean reachedLimit() {
			return this.reachedLimit;
		}

		public void seek(long absolutePos) throws IOException {
			this.relativePos = absolutePos - start;
			this.stream.seek(absolutePos);
			// mark as empty
			this.charBuffer.limit(0);
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#read(char[], int, int)
		 */
		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			int toRead = len - off;
			while (toRead > 0) {
				fillCharBufferIfEmpty();
				int currentReadCount = Math.min(toRead, this.charBuffer.length());
				this.charBuffer.get(cbuf, off, currentReadCount);
				toRead -= currentReadCount;
			}
			return len - toRead;
		}

		private void fillCharBufferIfEmpty() throws IOException {
			if (this.charBuffer.remaining() == 0) {
				final int maxLen = this.streamBuffer.capacity();
				this.streamBuffer.clear();
				if (this.reachedLimit) {
					final int read = this.stream.read(this.streamBuffer.array(), 0, maxLen);
					this.streamBuffer.limit(read);
				} else {
					final int read =
						this.stream.read(this.streamBuffer.array(), 0, (int) Math.min(maxLen, this.limit
							- this.relativePos));
					this.relativePos += read;
					this.streamBuffer.limit(read);
					this.reachedLimit = this.limit <= this.relativePos;
				}

				this.charBuffer.clear();
				this.decoder.decode(this.streamBuffer, this.charBuffer, false);
				this.charBuffer.flip();
			}
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#read()
		 */
		@Override
		public int read() throws IOException {
			fillCharBufferIfEmpty();
			if (this.charBuffer.remaining() == 0)
				return -1;
			return this.charBuffer.get();
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.Reader#close()
		 */
		@Override
		public void close() throws IOException {
			this.stream.close();
		}
	}

	/**
	 * The default number of sample lines to consider when calculating the line width.
	 */
	private static final int DEFAULT_NUM_SAMPLES = 10;

	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 */
	public static final String NUM_STATISTICS_SAMPLES = "csv-format.numSamples";

	public static final String COLUMN_NAMES = "csv-format.columns";

	public static final String USE_QUOTATION = "csv-format.quotation";

	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(CsvInputFormat.class);

	private static final char DEFAULT_DELIMITER = ',';

	private char fieldDelimiter = ',';

	private Quotation quotation = Quotation.AUTO;

	private boolean usesQuotation = true;

	private String[] keyNames;

	private Schema targetSchema;

	private Charset encoding;

	private int numLineSamples;

	public enum Quotation {
		ON, OFF, AUTO;
	}

	private enum State {
		TOP_LEVEL, QUOTED, ESCAPED;
	}

	private Deque<State> state = new LinkedList<State>();

	private boolean endReached;

	private CountingReader reader;

	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);

		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.targetSchema = this.context.getOutputSchema(0);

		final Boolean useQuotation = SopremoUtil.deserialize(parameters, USE_QUOTATION, Boolean.class);
		this.quotation = useQuotation == null ? Quotation.AUTO : useQuotation ? Quotation.ON : Quotation.OFF;
		this.keyNames = SopremoUtil.deserialize(parameters, COLUMN_NAMES, String[].class);
		// this.targetSchema = SopremoUtil.deserialize(parameters, SCHEMA, Schema.class);
		this.encoding = Charset.forName(parameters.getString(ENCODING, "utf-8"));
		final Character delimiter = SopremoUtil.deserialize(parameters, FIELD_DELIMITER, Character.class);
		this.fieldDelimiter = delimiter != null ? delimiter : DEFAULT_DELIMITER;

		this.numLineSamples = parameters.getInteger(NUM_STATISTICS_SAMPLES, DEFAULT_NUM_SAMPLES);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		this.setState(State.TOP_LEVEL);

		this.endReached = false;
		this.reader = new CountingReader(this.stream, this.encoding, this.splitStart, this.splitLength);
		this.usesQuotation = this.quotation == Quotation.ON;
		if (this.quotation == Quotation.AUTO) {
			// very simple heuristic
			for (int index = 0, ch; !this.usesQuotation && index < 1000 && (ch = this.reader.read()) != -1; index++)
				this.usesQuotation = ch == '"';

			this.reader.seek(this.splitStart);
		}

		if (this.keyNames == null) {
			if (split.getSplitNumber() > 0)
				this.reader.seek(0);
			this.keyNames = this.extractKeyNames();
		}

		// skip to beginning of the first record
		if (split.getSplitNumber() > 0) {
			if (this.usesQuotation) {
				// TODO: how to detect if where are inside a quotation?
				this.reader.seek(this.splitStart - 1);

				int ch;
				while ((ch = this.reader.read()) != -1 && ch != '\n')
					;
				this.endReached = ch == -1;
			} else {
				this.reader.seek(this.splitStart - 1);

				int ch;
				while ((ch = this.reader.read()) != -1 && ch != '\n')
					;
				this.endReached = ch == -1;
			}
		}
	}

	/**
	 * Reads the key names from the first line of the first split.
	 */
	private String[] extractKeyNames() throws IOException {
		List<String> keyNames = new ArrayList<String>();
		int lastCharacter;
		do {
			lastCharacter = this.fillBuilderWithNextField();
			keyNames.add(this.builder.toString());
			this.builder.setLength(0);
		} while (lastCharacter != -1 && lastCharacter != '\n');

		return keyNames.toArray(new String[keyNames.size()]);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#reachedEnd()
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return this.endReached;
	}

	private final IObjectNode objectNode = new ObjectNode();

	private final StringBuilder builder = new StringBuilder();

	private int fillBuilderWithNextField() throws IOException {
		int character = 0;
		readLoop: while ((character = this.reader.read()) != -1) {
			final char ch = (char) character;
			switch (this.getCurrentState()) {
			case ESCAPED:
				this.builder.append(ch);
				this.revertToPreviousState();
				break;
			case QUOTED:
				switch (ch) {
				case '"':
					this.revertToPreviousState();
					break;
				case '\\':
					this.setState(State.ESCAPED);
					break;
				default:
					this.builder.append(ch);
				}
				break;
			case TOP_LEVEL:
				if (ch == this.fieldDelimiter) {
					this.builder.toString();
					break readLoop;
				} else if (ch == '\n') {
					final int lastCharPos = this.builder.length() - 1;
					if (this.builder.charAt(lastCharPos) == '\r')
						this.builder.setLength(lastCharPos);
					break readLoop;
				} else if (this.usesQuotation && ch == '"')
					this.setState(State.QUOTED);
				else
					this.builder.append(ch);
			}
		}
		return character;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException {
		int lastCharacter, fieldIndex = 0;
		do {
			lastCharacter = this.fillBuilderWithNextField();
			// ignore empty line
			if (lastCharacter == 0 && fieldIndex == 0 && this.builder.length() == 0)
				break;
			this.addToObject(fieldIndex++, this.builder.toString());
			this.builder.setLength(0);
		} while (lastCharacter != -1 && lastCharacter != '\n');

		if (this.objectNode.size() == 0)
			return false;
		this.targetSchema.jsonToRecord(this.objectNode, record, this.context);
		this.endReached = lastCharacter == -1 || this.reader.reachedLimit();
		return true;
	}

	/**
	 * @param escaped
	 */
	private void setState(State newState) {
		this.state.push(newState);
	}

	/**
	 * @param fieldIndex
	 * @param string
	 */
	private void addToObject(int fieldIndex, String string) {
		this.objectNode.put(this.keyNames[fieldIndex], TextNode.valueOf(string));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		this.revertToPreviousState();
		this.reader.close();
		super.close();
	}

	/**
	 * @return
	 */
	private State getCurrentState() {
		return this.state.peek();
	}

	private State revertToPreviousState() {
		return this.state.pop();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#getStatistics(eu.stratosphere.pact.common.io.statistics.
	 * BaseStatistics)
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		// check the cache
		FileBaseStatistics stats = null;

		if (cachedStatistics != null && cachedStatistics instanceof FileBaseStatistics)
			stats = (FileBaseStatistics) cachedStatistics;
		else
			stats = new FileBaseStatistics(-1, BaseStatistics.UNKNOWN, BaseStatistics.UNKNOWN);

		try {
			final Path file = this.filePath;
			final URI uri = file.toUri();

			// get the filesystem
			final FileSystem fs = FileSystem.get(uri);
			List<FileStatus> files = null;

			// get the file info and check whether the cached statistics are still
			// valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = true;

					for (FileStatus s : fss)
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.getLastModificationTime()) {
								stats.setLastModificationTime(s.getModificationTime());
								unmodified = false;
							}
						}

					if (unmodified)
						return stats;
				}
				else {
					// check if the statistics are up to date
					long modTime = status.getModificationTime();
					if (stats.getLastModificationTime() == modTime)
						return stats;

					stats.setLastModificationTime(modTime);

					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			long fileSize = 0;

			// calculate the whole length
			for (FileStatus s : files)
				fileSize += s.getLen();

			// sanity check
			if (fileSize <= 0) {
				fileSize = BaseStatistics.UNKNOWN;
				return stats;
			}
			stats.setTotalInputSize(fileSize);

			// make the samples small for very small files
			int numSamples = Math.min(this.numLineSamples, (int) (fileSize / 1024));
			if (numSamples < 2)
				numSamples = 2;

			long offset = 0;
			long bytes = 0; // one byte for the line-break
			long stepSize = fileSize / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < files.size(); sampleNum++) {
				FileStatus currentFile = files.get(fileNum);
				FSDataInputStream inStream = null;

				try {
					inStream = fs.open(currentFile.getPath());
					LineReader lineReader = new LineReader(inStream, offset, currentFile.getLen() - offset, 1024);
					byte[] line = lineReader.readLine();
					lineReader.close();

					if (line != null && line.length > 0) {
						samplesTaken++;
						bytes += line.length + 1; // one for the linebreak
					}
				} finally {
					// make a best effort to close
					if (inStream != null)
						try {
							inStream.close();
						} catch (Throwable t) {
						}
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < files.size() && offset >= (currentFile = files.get(fileNum)).getLen()) {
					offset -= currentFile.getLen();
					fileNum++;
				}
			}

			stats.setAverageRecordWidth(bytes / (float) samplesTaken);
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled())
				LOG.warn("Could not determine complete statistics for file '" + this.filePath
					+ "' due to an io error: "
					+ ioex.getMessage());
		} catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error("Unexpected problen while getting the file statistics for file '" + this.filePath + "': "
					+ t.getMessage(), t);
		}

		return stats;
	}

}
