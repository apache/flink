package eu.stratosphere.pact.programs.preparation.tasks;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class AdjListOutput extends FileOutputFormat {

	LongList adjList = new LongList();
	
	@Override
	public void writeRecord(PactRecord record) throws IOException {
		stream.write((record.getField(0, PactLong.class).getValue()+"").getBytes());
		stream.write('|');
		
		adjList = record.getField(1, adjList);
		long[] neighbours = adjList.getList();
		int numNeighbours = adjList.getLength();
		
		for (int i = 0; i < numNeighbours; i++) {
			stream.write((neighbours[i]+"").getBytes());
			
			if(i < (numNeighbours-1)) {
				stream.write(',');
			}
		}
		stream.write('\n');
	}

}
