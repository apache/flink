package eu.stratosphere.pact.programs.pagerank.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class VertexPageRank implements Value {
	private static final VertexPageRankAccessor accessor =
			new VertexPageRankAccessor();
	
	protected long vid;
	protected double rank;
	
	public VertexPageRank() {
		
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		accessor.serialize(this, out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		accessor.deserialize(this, in);
	}


	public long getVid() {
		return vid;
	}


	public void setVid(long vid) {
		this.vid = vid;
	}


	public double getRank() {
		return rank;
	}


	public void setRank(double rank) {
		this.rank = rank;
	}

}
