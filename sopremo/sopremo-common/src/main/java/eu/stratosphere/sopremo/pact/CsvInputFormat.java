package eu.stratosphere.sopremo.pact;

import java.io.BufferedReader;
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

public class CsvInputFormat extends FileInputFormat<PactJsonObject.Key, PactJsonObject>{

//	private boolean array;
	
	private BufferedReader reader;
	
	private String splitsymbol = ",";
	private String typeidentifier = "\"";
	
	private String currentLine;
	
	private List<String> keynames = new ArrayList<String>();

	private boolean end;
	
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}
	
	private void checkEnd() throws IOException {
		currentLine= this.reader.readLine();
		if(this.currentLine != null){
			this.end = false;
		} else this.end = true;		
	}
	
	@Override
	public boolean reachedEnd() {
		return this.end;
	}
	

	@Override
	public boolean nextRecord(KeyValuePair<Key, PactJsonObject> record) throws IOException {
		if (!this.end) {
			
			record.getValue().setValue(this.parseCurrentLine(this.currentLine));
			this.checkEnd();
			return true;
		}

		return false;
	}
	

	private JsonNode parseCurrentLine(String currentLine) {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		String[] split = currentLine.replaceAll(this.typeidentifier, "").split(this.splitsymbol);
		for(int i=0; i<split.length;i++){
			node.put(this.keynames.get(i), split[i]);
		}
		return node;
	}

	@Override
	public KeyValuePair<PactJsonObject.Key, PactJsonObject> createPair() {
		return new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL,
				new PactJsonObject());
	}
	

	@Override
	public void open(FileInputSplit split) throws  IOException {
		super.open(split);
		
		this.end = false;
		this.reader = new BufferedReader(new InputStreamReader(this.stream));
		String firstline = this.reader.readLine();
		if(firstline != null){
			String[] splitLine = firstline.replaceAll(this.typeidentifier, "").split(this.splitsymbol);
			for(int i=0;i<splitLine.length;i++){
				keynames.add(splitLine[i]);
			}
			this.checkEnd();
		} else this.end=true;
		
	}
}
