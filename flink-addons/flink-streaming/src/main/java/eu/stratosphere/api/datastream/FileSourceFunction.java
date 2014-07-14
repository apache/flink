package eu.stratosphere.api.datastream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class FileSourceFunction extends SourceFunction<Tuple1<String>> {
	private static final long serialVersionUID = 1L;
	
	private final String path;
	private Tuple1<String> outTuple = new Tuple1<String>();
	
	public FileSourceFunction(String path) {
		this.path = path;
	}
	
	@Override
	public void invoke(Collector<Tuple1<String>> collector) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while (line != null) {
			if (line != "") {
				outTuple.f0 = line;
				collector.collect(outTuple);
			}
			line = br.readLine();
		}
		br.close();
	}

}
