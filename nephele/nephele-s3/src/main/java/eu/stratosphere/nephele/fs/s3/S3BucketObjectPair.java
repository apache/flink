package eu.stratosphere.nephele.fs.s3;

public final class S3BucketObjectPair {

	private final String bucket;

	private final String object;

	S3BucketObjectPair(final String bucket, final String object) {
		this.bucket = bucket;
		this.object = object;
	}

	public String getBucket() {
		return this.bucket;
	}

	public String getObject() {
		return this.object;
	}
	
	public boolean hasBucket() {
		return (this.bucket != null);
	}
	
	public boolean hasObject() {
		return (this.object != null);
	}
}
