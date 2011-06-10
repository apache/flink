package eu.stratosphere.nephele.fs.s3;

import java.io.IOException;

import eu.stratosphere.nephele.fs.BlockLocation;

public final class S3BlockLocation implements BlockLocation {

	private final String[] hosts;
	
	private final long length;
	
	S3BlockLocation(final String host, final long length) {
		
		this.hosts = new String[1];
		this.hosts[0] = host;
		this.length = length;
	}
	
	@Override
	public int compareTo(BlockLocation arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getHosts() throws IOException {

		return this.hosts;
	}

	@Override
	public long getOffset() {

		return 0;
	}

	@Override
	public long getLength() {
		
		return this.length;
	}

}
