# Native S3 FileSystem

This module provides a native S3 filesystem implementation for Apache Flink using AWS SDK v2.

## Overview

The Native S3 FileSystem is a direct implementation of Flink's FileSystem interface using AWS SDK v2, without Hadoop dependencies. It provides exactly-once semantics for checkpointing and file sinks through S3 multipart uploads.

## Supported URI Schemes

This module supports both `s3://` and `s3a://` URI schemes:

| Scheme | Description |
|--------|-------------|
| `s3://` | Primary scheme for native S3 filesystem |
| `s3a://` | Hadoop S3A compatibility scheme - allows drop-in replacement for existing Hadoop-based configurations |

Both schemes use the same native AWS SDK v2 implementation and share identical configuration options.

**Example usage with either scheme:**

```java
// Using s3:// scheme
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/checkpoints");

// Using s3a:// scheme (for Hadoop compatibility)
env.getCheckpointConfig().setCheckpointStorage("s3a://my-bucket/checkpoints");
```

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

### Core Settings

| Key | Default | Description |
|-----|---------|-------------|
| s3.access-key | (none) | AWS access key |
| s3.secret-key | (none) | AWS secret key |
| s3.region | (auto-detect) | AWS region (auto-detected via AWS_REGION, ~/.aws/config, EC2 metadata) |
| s3.endpoint | (none) | Custom S3 endpoint (for MinIO, LocalStack, etc.) |
| s3.path-style-access | false | Use path-style access (auto-enabled for custom endpoints) |
| s3.upload.min.part.size | 5242880 | Minimum part size for multipart uploads (5MB) |
| s3.upload.max.concurrent.uploads | CPU cores | Maximum concurrent uploads per stream |
| s3.entropy.key | (none) | Key for entropy injection in paths |
| s3.entropy.length | 4 | Length of entropy string |
| s3.bulk-copy.enabled | true | Enable bulk copy operations |
| s3.async.enabled | true | Enable async read/write with TransferManager |
| s3.read.buffer.size | 262144 (256KB) | Read buffer size per stream (64KB - 4MB) |

### Server-Side Encryption (SSE)

| Key | Default | Description |
|-----|---------|-------------|
| s3.sse.type | none | Encryption type: `none`, `sse-s3` (AES256), `sse-kms` (AWS KMS) |
| s3.sse.kms.key-id | (none) | KMS key ID/ARN/alias for SSE-KMS (uses default aws/s3 key if not specified) |

### IAM Assume Role

| Key | Default | Description |
|-----|---------|-------------|
| s3.assume-role.arn | (none) | ARN of the IAM role to assume |
| s3.assume-role.external-id | (none) | External ID for cross-account access |
| s3.assume-role.session-name | flink-s3-session | Session name for the assumed role |
| s3.assume-role.session-duration | 3600 | Session duration in seconds (900-43200) |

## Server-Side Encryption (SSE)

The filesystem supports server-side encryption for data at rest:

### SSE-S3 (S3-Managed Keys)

Amazon S3 manages the encryption keys. Simplest option with no additional configuration.

```yaml
s3.sse.type: sse-s3
```

All objects will be encrypted with AES-256 using keys managed by S3.

### SSE-KMS (AWS KMS-Managed Keys)

Use AWS Key Management Service for encryption key management. Provides additional security features like key rotation, audit trails, and fine-grained access control.

**Using the default aws/s3 key:**
```yaml
s3.sse.type: sse-kms
```

**Using a custom KMS key:**
```yaml
s3.sse.type: sse-kms
s3.sse.kms.key-id: arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789abc
# Or use an alias:
# s3.sse.kms.key-id: alias/my-s3-encryption-key
```

**Using encryption context (for fine-grained IAM access control):**

Encryption context allows you to add additional authenticated data (AAD) to KMS encrypt/decrypt operations.
This enables IAM policies to restrict access based on context values:

```yaml
s3.sse.type: sse-kms
s3.sse.kms.key-id: alias/my-key
# Configure encryption context as key-value pairs
s3.sse.kms.encryption-context.department: finance
s3.sse.kms.encryption-context.project: budget-reports
```

With encryption context, you can create IAM policies like:
```json
{
  "Effect": "Allow",
  "Action": ["kms:Encrypt", "kms:GenerateDataKey"],
  "Resource": "arn:aws:kms:region:account:key/key-id",
  "Condition": {
    "StringEquals": {
      "kms:EncryptionContext:department": "finance"
    }
  }
}
```

See [AWS KMS Encryption Context](https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html) for more details.

**Note:** Ensure the IAM role/user has `kms:Encrypt` and `kms:GenerateDataKey` permissions on the KMS key.

## IAM Assume Role

For cross-account access or temporary elevated permissions, configure an IAM role to assume:

### Basic Assume Role

```yaml
s3.assume-role.arn: arn:aws:iam::123456789012:role/S3AccessRole
```

### Cross-Account Access with External ID

For enhanced security when granting access to third parties:

```yaml
s3.assume-role.arn: arn:aws:iam::123456789012:role/CrossAccountS3Role
s3.assume-role.external-id: your-secret-external-id
s3.assume-role.session-name: flink-cross-account-session
s3.assume-role.session-duration: 3600  # 1 hour
```

### How It Works

1. Flink uses base credentials (access key, environment, or IAM role) to call STS AssumeRole
2. STS returns temporary credentials (access key, secret key, session token)
3. All S3 operations use the assumed role's permissions
4. Credentials are automatically refreshed before expiration

**IAM Policy Example for the Assumed Role:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

**Trust Policy for Cross-Account Access:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::SOURCE_ACCOUNT_ID:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "your-secret-external-id"
        }
      }
    }
  ]
}
```

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
3. **Allocate sufficient JVM heap** for FS operations
4. **Monitor**: `s3.read.buffer.size` × `parallelism` = peak FS memory

**Important Note on Memory Types:**
- The filesystem uses **JVM heap memory** for read/write buffers
- This is separate from RocksDB's **managed native memory** used for state backend
- Flink's `taskmanager.memory.managed.size` controls RocksDB native memory, NOT filesystem buffers
- Increase JVM heap (`taskmanager.memory.task.heap.size`) if you see OOM errors during S3 operations

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
