package eu.stratosphere.sopremo.pact;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.csvreader.CsvReader;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;

public class CsvInputFormat extends TextInputFormat<PactJsonObject.Key, PactJsonObject> {

	private static final String FIELD_DELIMITER = "fieldDelimiter";

	public static final String COLUMN_NAMES = "columnNames";

	private char fieldDelimiter = ',';

	private String[] keyNames;

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);
		this.keyNames = SopremoUtil.deserialize(parameters, COLUMN_NAMES, String[].class);
		final Character delimiter = SopremoUtil.deserialize(parameters, FIELD_DELIMITER, Character.class);
		if (delimiter != null)
			this.fieldDelimiter = delimiter;
	}

	@Override
	public KeyValuePair<PactJsonObject.Key, PactJsonObject> createPair() {
		return new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL,
			new PactJsonObject());
	}

	@Override
	public void open(final FileInputSplit split) throws IOException {
		super.open(split);

		// this.end = false;
		// this.reader = new CsvReader(new InputStreamReader(this.stream, "UTF8"));
		// this.reader.setDelimiter(this.fieldDelimiter);
		//
		// if (this.keyNames == null) {
		// this.reader.readHeaders();
		// this.keyNames = this.reader.getHeaders();
		//
		// // for any reason, there is a BOM symbol in front of the first character
		// if (keyNames.length > 0)
		// this.keyNames[0] = this.keyNames[0].replaceAll("^\\ufeff", "");
		// }
	}

	//
	// @Override
	// public boolean reachedEnd() {
	// return this.end;
	// }

	private final Charset charSet = Charset.forName("utf-8");

	@Override
	public boolean readLine(final KeyValuePair<Key, PactJsonObject> pair, final byte[] record) {
		// if (!this.end) {
		final CsvReader reader = new CsvReader(new ByteArrayInputStream(record), this.charSet);
		reader.setDelimiter(this.fieldDelimiter);
		try {
			if (reader.readRecord()) {
				final ObjectNode node = new ObjectNode();
				if (this.keyNames != null)
					for (int i = 0; i < this.keyNames.length; i++)
						node.put(this.keyNames[i], reader.get(i));
				else
					for (int i = 0; i < reader.getColumnCount(); i++)
						node.put(String.format("key%d", i + 1), reader.get(i));
				pair.getValue().setValue(node);
				return true;
			}

			// this.end = true;
		} catch (final IOException e) {
			SopremoUtil.LOG.warn("Parsing CSV record", e);
		}
		return false;
		// }
		// return false;
	}
}
