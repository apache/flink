package eu.stratosphere.sopremo.pact;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;

import com.csvreader.CsvReader;

public class CsvInputFormat extends FileInputFormat<PactJsonObject.Key, PactJsonObject> {

	private CsvReader reader;

	private char fieldDelimiter = ',';

	private List<String> keynames = new ArrayList<String>();

	private boolean end;

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}

	private void checkEnd() throws IOException {
		if (reader.readRecord()) {
			this.end = false;
		} else {
			this.end = true;
			reader.close();
		}
	}

	@Override
	public boolean reachedEnd() {
		return this.end;
	}

	@Override
	public boolean nextRecord(KeyValuePair<Key, PactJsonObject> record) throws IOException {
		if (!this.end) {

			record.getValue().setValue(this.parseCurrentLine());
			this.checkEnd();
			return true;
		}

		return false;
	}

	private JsonNode parseCurrentLine() throws IOException {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		for (int i = 0; i < keynames.size(); i++) {
			node.put(keynames.get(i), reader.get(keynames.get(i)));
		}
		return node;
	}

	@Override
	public KeyValuePair<PactJsonObject.Key, PactJsonObject> createPair() {
		return new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL,
			new PactJsonObject());
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		this.end = false;
		this.reader = new CsvReader(new InputStreamReader(this.stream, "UTF8"));
		reader.setDelimiter(this.fieldDelimiter);
		reader.readHeaders();
		String[] headers = reader.getHeaders();
		for (int i = 0; i < headers.length; i++) {
			keynames.add(headers[i]);
		}

		// for any reason, there is a BOM symbol in front of the first character
		keynames.set(0, keynames.get(0).replaceAll("^\\ufeff", ""));
		headers[0] = headers[0].replaceAll("^\\ufeff", "");
		reader.setHeaders(headers);

		this.checkEnd();

	}
}
