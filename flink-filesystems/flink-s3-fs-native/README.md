# Native S3 FileSystem

This module provides a native S3 filesystem implementation for Apache Flink using AWS SDK v2.

## Overview

The Native S3 FileSystem is a direct implementation of Flink's FileSystem interface using AWS SDK v2, without Hadoop dependencies. It provides exactly-once semantics for checkpointing and file sinks through S3 multipart uploads.

## Usage

Add this module to Flink's plugins directory:

```bash
mkdir -p $FLINK_HOME/plugins/s3-fs-native
cp flink-s3-fs-native-*.jar $FLINK_HOME/plugins/s3-fs-native/
```

Configure S3 credentials in `conf/config.yaml`:

```yaml
s3.access-key: YOUR_ACCESS_KEY
s3.secret-key: YOUR_SECRET_KEY
s3.endpoint: https://s3.amazonaws.com  # Optional, defaults to AWS
```

Use S3 paths in your Flink application:

```java
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/checkpoints");

DataStream<String> input = env.readTextFile("s3://my-bucket/input");
input.sinkTo(FileSink.forRowFormat(new Path("s3://my-bucket/output"), 
                                    new SimpleStringEncoder<>()).build());
```

## Configuration Options

| Key | Default | Description |
|-----|---------|-------------|
| s3.access-key | (none) | AWS access key |
| s3.secret-key | (none) | AWS secret key |
| s3.region | us-east-1 | AWS region |
| s3.endpoint | (none) | Custom S3 endpoint (for MinIO, LocalStack, etc.) |
| s3.path-style-access | false | Use path-style access (auto-enabled for custom endpoints) |
| s3.upload.min.part.size | 5242880 | Minimum part size for multipart uploads (5MB) |
| s3.upload.max.concurrent.uploads | CPU cores | Maximum concurrent uploads per stream |
| s3.entropy.key | (none) | Key for entropy injection in paths |
| s3.entropy.length | 4 | Length of entropy string |
| s3.bulk-copy.enabled | true | Enable bulk copy operations |
| s3.async.enabled | true | Enable async read/write with TransferManager |
| s3.read.buffer.size | 262144 (256KB) | Read buffer size per stream (64KB - 4MB) |

## MinIO and S3-Compatible Storage

The filesystem auto-detects custom endpoints and configures appropriate settings:

```yaml
s3.access-key: minioadmin
s3.secret-key: minioadmin  
s3.endpoint: http://localhost:9000
s3.path-style-access: true  # Auto-enabled for custom endpoints
```

MinIO-specific optimizations are applied automatically:
- Path-style access enabled
- Chunked encoding disabled (compatibility)
- Checksum validation disabled (compatibility)

## Memory Optimization for Large Files

The filesystem is optimized to handle large files without OOM errors:

### Streaming Reads (No Buffering)
- Files are **streamed** chunk-by-chunk, not loaded into memory
- Configurable read buffer (default 256KB) prevents memory spikes
- Only small buffer held in memory at any time

### Configuration for Memory Efficiency

```yaml
# Read buffer: smaller = less memory, larger = better throughput
s3.read.buffer.size: 262144  # 256KB (default)
# For memory-constrained environments: 65536 (64KB)
# For high-throughput: 1048576 (1MB)
```

**Memory Calculation Per Parallel Read:**
- Buffer size × concurrent reads = total memory
- Example: 256KB buffer × 16 parallel readers = 4MB total
- This allows processing GB-sized files with MB-sized memory

### OOM Prevention Strategies

1. **Use smaller read buffers** (64-128KB) for very large files
2. **Reduce parallelism** to limit concurrent S3 readers
3. **Enable managed memory** for Flink state backend
4. **Monitor**: `s3.read.buffer.size` × `parallelism` = peak memory

## Async Operations with TransferManager

The filesystem uses AWS SDK's TransferManager for high-performance async read/write operations:

**Benefits:**
- **Automatic multipart management**: TransferManager automatically handles multipart uploads for large files
- **Parallel transfers**: Multiple parts uploaded concurrently for maximum throughput  
- **Progress tracking**: Built-in progress monitoring and retry logic
- **Resource optimization**: Efficient connection pooling and memory management
- **Streaming uploads**: Data streamed from disk, not buffered in memory

**Configuration:**
```yaml
s3.async.enabled: true  # Default: enabled
```

When enabled, file uploads automatically use TransferManager for:
- Large file uploads (automatic multipart handling)
- Checkpoint data writes
- Recoverable output stream operations

**Performance Impact:**
- Up to 10x faster uploads for large files (>100MB)
- **Reduced memory pressure** through streaming
- Better utilization of available bandwidth
- Lower heap requirements for write operations

## Checkpointing

Configure checkpoint storage in `conf/config.yaml`:

```yaml
state.checkpoints.dir: s3://my-bucket/checkpoints
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
```

Or programmatically:

```java
env.enableCheckpointing(10000);
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/checkpoints");
```

## Entropy Injection

For high-throughput checkpointing to avoid S3 hot partitions:

```yaml
s3.entropy.key: _entropy_
s3.entropy.length: 4
```

Paths like `s3://bucket/_entropy_/checkpoints` will be expanded to `s3://bucket/af7e/checkpoints` with random characters.

## Implementation Details

The filesystem uses:
- **AWS SDK v2** for all S3 operations
- **Multipart uploads** for recoverable writes and large files
- **S3 Transfer Manager** for bulk copy operations  
- **Separate sync and async clients** for optimal performance

Key classes:
- `NativeS3FileSystem` - Main FileSystem implementation
- `NativeS3RecoverableWriter` - Exactly-once writer using multipart uploads
- `S3ClientProvider` - Manages S3 client lifecycle
- `NativeS3AccessHelper` - Low-level S3 operations

## Building

```bash
mvn clean package
```

## Testing with MinIO

```bash
# Start MinIO
docker run -d -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"

# Create bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/test-bucket

# Run Flink with MinIO
export FLINK_HOME=/path/to/flink
cat > $FLINK_HOME/conf/config.yaml <<EOF
s3.endpoint: http://localhost:9000
s3.access-key: minioadmin
s3.secret-key: minioadmin
s3.path-style-access: true
EOF

$FLINK_HOME/bin/flink run YourJob.jar
```

## Delegation Tokens

The filesystem supports delegation tokens for secure multi-tenant deployments. The delegation token service name is `s3-native` to avoid conflicts with other S3 filesystem implementations.

Configure delegation tokens:

```yaml
security.delegation.token.provider.s3-native.enabled: true
security.delegation.token.provider.s3-native.access-key: YOUR_KEY
security.delegation.token.provider.s3-native.secret-key: YOUR_SECRET
security.delegation.token.provider.s3-native.region: us-east-1
```
