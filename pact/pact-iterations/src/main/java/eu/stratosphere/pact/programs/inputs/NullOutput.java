package eu.stratosphere.pact.programs.inputs;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public class NullOutput extends OutputFormat {

	@Override
	public void configure(Configuration parameters) {
		// TODO Auto-generated method stub

	}

	@Override
	public void open(int taskNumber) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
