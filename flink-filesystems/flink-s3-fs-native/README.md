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
| s3.access-key | (none) | AWS access key (fallback key: `s3.access.key`) |
| s3.secret-key | (none) | AWS secret key (fallback key: `s3.secret.key`) |
| s3.region | (auto-detect) | AWS region (auto-detected via AWS_REGION, ~/.aws/config, EC2 metadata) |
| s3.endpoint | (none) | Custom S3 endpoint (for SeaweedFS, LocalStack, etc.) |
| s3.path-style-access | false | Use path-style access for S3 (required by most S3-compatible servers such as SeaweedFS; fallback key: `s3.path.style.access`) |
| s3.chunked-encoding.enabled | true | Enable chunked encoding for S3 requests. Disable for S3-compatible servers that do not support it |
| s3.checksum-validation.enabled | true | Enable checksum validation for S3 requests. Disable for S3-compatible servers that do not support it |
| s3.upload.min.part.size | 5242880 | Minimum part size for multipart uploads (5MB to 5GB) |
| s3.upload.max.concurrent.uploads | CPU cores | Maximum concurrent part uploads per stream |
| s3.entropy.key | (none) | Key for entropy injection in paths |
| s3.entropy.length | 4 | Length of entropy string |
| s3.bulk-copy.enabled | true | Enable bulk copy operations for S3-to-local downloads |
| s3.bulk-copy.max-concurrent | 16 | Maximum number of concurrent copy operations |
| s3.bulk-copy.download-buffer-size | 262144 (256KB) | Buffer size for writing bulk-copy downloads to local disk. Bounds the JDK's cached temporary direct buffers, preventing direct-memory OutOfMemoryError during large RocksDB state restores |
| s3.connection.max | 50 | Maximum HTTP connections in the S3 client connection pool. Applies to sync and async clients, including CRT when enabled. Must be ≥ `s3.bulk-copy.max-concurrent` |
| s3.async.enabled | true | Enable async read/write with TransferManager |
| s3.read.buffer.size | 262144 (256KB) | Read buffer size per stream (64KB - 4MB) |

### Credentials Provider

| Key | Default | Description |
|-----|---------|-------------|
| fs.s3.aws.credentials.provider | (none) | Comma-separated list of AWS credentials provider class names. Providers are tried in order; the first one that returns credentials is used. Supports fully-qualified AWS SDK v2 class names (e.g. `software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider`) or simple names from the SDK auth package (e.g. `AnonymousCredentialsProvider`, `DefaultCredentialsProvider`). When not set, the default chain is used: delegation tokens → static credentials (if configured) → `DefaultCredentialsProvider` |

### Retries and Timeouts

| Key | Default | Description |
|-----|---------|-------------|
| s3.retry.max-num-retries | 3 | Maximum retry attempts for failed S3 requests. Uses the AWS SDK's default retry strategy (exponential backoff with jitter). Set to `0` to disable retries |
| s3.connection.timeout | 60s | HTTP connection timeout for the S3 client (time to establish a connection) |
| s3.socket.timeout | 60s | HTTP socket timeout for the S3 client (time to wait for data after connection is established) |
| s3.connection.max-idle-time | 60s | Maximum idle time for HTTP connections in the connection pool |
| s3.close.timeout | 60s | Timeout for closing the S3 filesystem (waiting for pending operations to complete during shutdown) |
| s3.client.close.timeout | 30s | Timeout for closing the S3 client and releasing resources |

### Server-Side Encryption (SSE)

| Key | Default | Description |
|-----|---------|-------------|
| s3.sse.type | none | Encryption type. Accepted values: `none`, `sse-s3` or `aes256` (S3-managed keys), `sse-kms` or `aws:kms` (KMS-managed keys). The `aes256` and `aws:kms` aliases match Hadoop S3A's `fs.s3a.server-side-encryption-algorithm` values to ease migration |
| s3.sse.kms.key-id | (none) | KMS key ID/ARN/alias for SSE-KMS (uses default aws/s3 key if not specified) |
| s3.sse.kms.encryption-context | (none) | Encryption context key-value pairs for SSE-KMS. Format: `key1:value1,key2:value2`. Keys/values containing `:` must be quoted. |

### IAM Assume Role

| Key | Default | Description |
|-----|---------|-------------|
| s3.assume-role.arn | (none) | ARN of the IAM role to assume |
| s3.assume-role.external-id | (none) | External ID for cross-account access |
| s3.assume-role.session-name | flink-s3-session | Session name for the assumed role |
| s3.assume-role.session-duration | 3600 | Session duration in seconds (900-43200) |

## Bucket-Level Configuration

The Native S3 FileSystem supports per-bucket configuration overrides, allowing different S3 buckets to use different connection settings within the same Flink cluster. This enables scenarios like:

- **Different credentials per bucket** (e.g., cross-account access for a data sink bucket)
- **Different regions or endpoints** (e.g., checkpoints in `us-east-1`, archive bucket in `eu-west-1`)
- **Bucket-specific encryption** (e.g., SSE-KMS for sensitive data, SSE-S3 for logs)

### Format

Bucket-level configuration uses the format: `s3.bucket.<bucket-name>.<property>`

Bucket names containing dots (e.g., `my.company.data`) are fully supported through longest-suffix matching.

> **Note:** AWS recommends avoiding periods (`.`) in bucket names. Buckets with dots cannot use virtual-hosted-style addressing over HTTPS without custom certificate validation. If you use dotted bucket names, enable `path-style-access` for that bucket (see [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)).

### Supported Properties

Only the following properties can be overridden at the bucket level. Any other `s3.bucket.<bucket>.<prop>` key is ignored (a warning is logged):

- **Connection:** `region`, `endpoint`, `path-style-access`
- **Credentials:** `access-key`, `secret-key`, `aws.credentials.provider`
- **Encryption:** `sse.type`, `sse.kms.key-id`
- **IAM Assume Role:** `assume-role.arn`, `assume-role.external-id`, `assume-role.session-name`, `assume-role.session-duration`

Timeouts, retries, encoding/checksum flags, entropy, upload/copy settings, and the credentials provider chain are configured globally only.

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
s3.sse.kms.encryption-context: {"aws:s3:arn": "arn:aws:s3:::my-bucket/my-file"}
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

## SeaweedFS and S3-Compatible Storage

For S3-compatible servers (SeaweedFS, LocalStack, Ceph RGW, etc.), set the endpoint plus any compatibility flags the server requires. These flags are not auto-detected from the endpoint value — the defaults target AWS S3 and must be overridden explicitly:

```yaml
s3.endpoint: http://localhost:8333
fs.s3.aws.credentials.provider: AnonymousCredentialsProvider

# Required: SeaweedFS serves the S3 API in path-style and does not support
# virtual-hosted-style addressing by default.
s3.path-style-access: true

# Required: many S3-compatible servers do not support AWS chunked encoding or
# AWS-style checksum trailers used by the SDK by default.
s3.chunked-encoding.enabled: false
s3.checksum-validation.enabled: false
```

The exact subset of compatibility flags needed depends on the server and version — consult its documentation. The defaults in this filesystem (`path-style-access=false`, `chunked-encoding.enabled=true`, `checksum-validation.enabled=true`) are tuned for AWS S3.

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

## AWS Common Runtime (CRT) Support

The filesystem optionally supports the [AWS Common Runtime (CRT)](https://github.com/awslabs/aws-crt-java) HTTP transport
for higher throughput on large S3 workloads.

When enabled, the CRT transport replaces:
- **Sync client**: Apache HTTP Client → `AwsCrtHttpClient`
- **Async client**: Netty NIO → `S3AsyncClient.crtBuilder()` (with built-in multipart acceleration)

### Prerequisites

The `aws-crt` artifact contains JNI-linked native libraries whose C-side `FindClass` paths are
hardcoded, making Maven shade relocation incompatible. Therefore **the `aws-crt` JAR is not
bundled** in the fat JAR and must be placed manually.

### Setup

1. From the module directory, run the helper script to download the `aws-crt`
   JAR (auto-resolves the compatible version for the AWS SDK version this
   module was built against):

   ```bash
   ./tools/download-crt-jars.sh
   ```

   This places the JAR in `./crt-jars/`. Pass a different directory as the
   first argument if needed. Requires `mvn` on `PATH`.

2. Copy the JAR into the Flink plugin directory alongside `flink-s3-fs-native.jar`:

   ```bash
   cp crt-jars/aws-crt-*.jar $FLINK_HOME/plugins/s3-fs-native/
   ```

   > **Note:** `aws-crt-client` does **not** need to be downloaded or placed
   > separately — it is bundled (shaded) directly inside `flink-s3-fs-native.jar`
   > at build time. Only `aws-crt` (the JNI native lib) must be placed manually
   > because its C-side `FindClass` paths are hardcoded and incompatible with
   > Maven shade relocation.

3. Enable CRT in your Flink configuration (`conf/config.yaml`):

   ```yaml
   s3.crt.enabled: true
   ```

If the `aws-crt` JAR is missing when `s3.crt.enabled: true`, the filesystem
fails fast at startup with an `IllegalStateException` pointing back to this
setup procedure.

#### Manual download (alternative)

If `mvn` is unavailable, fetch the JAR by hand from Maven Central:

`aws-crt-<version>.jar` (groupId: `software.amazon.awssdk.crt`) — the version is
declared as the `<dependency>` on `software.amazon.awssdk.crt:aws-crt` inside
`aws-crt-client-<version>.pom` on Maven Central. The `aws-crt` artifact uses an
independent versioning scheme (e.g. `0.45.x`) that does **not** track the AWS SDK
version.

`aws-crt-client` does **not** need to be downloaded — it is bundled inside the fat JAR.

### CRT Configuration Options

| Key | Default | Description |
|-----|---------|-------------|
| s3.crt.enabled | false | Enable CRT HTTP transport for both sync and async S3 clients |
| s3.crt.target-throughput-gbps | (none) | Soft target throughput in Gbps for the CRT async client. Hint, not a hard cap — actual throughput may exceed the configured value. When unset, the AWS CRT runtime applies its own internal default; set this only to override it (e.g. to match the network bandwidth available to a single TaskManager). |
| s3.crt.max-native-memory-limit | (none) | Maximum native memory the CRT async client may use. When unset, the AWS CRT runtime applies its own internal limit. |
| s3.crt.read-buffer-size | (none) | Read buffer size for the CRT sync and async clients. Decoupled from `s3.read.buffer.size` so lowering the streaming read buffer does not shrink CRT's native transfer buffers. When unset, the AWS CRT runtime applies its own (larger) default. |
| s3.crt.max-concurrency | 256 | Max concurrent in-flight requests for the CRT sync and async clients. Decoupled from `s3.connection.max` because CRT fans one logical transfer into many part-sized requests; reusing the smaller connection-pool size causes "failed to acquire a connection" timeouts under parallel checkpoint upload/restore. |

The CRT read buffer is controlled independently via `s3.crt.read-buffer-size`; when unset, the CRT runtime keeps its own default rather than inheriting `s3.read.buffer.size`.

> **Note on options silently ignored under CRT:**
> - `s3.socket.timeout` — `AwsCrtHttpClient` has no socket-level read timeout; the CRT runtime uses `ConnectionHealthConfiguration` for stalled-read detection instead.
> - `s3.chunked-encoding.enabled` — `S3CrtAsyncClientBuilder` manages wire encoding internally and exposes no equivalent setter.

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
- `NativeS3ObjectOperations` - Low-level S3 operations (multipart upload, put, get, delete)

## Building

```bash
mvn clean package
```

## Testing with SeaweedFS

```bash
# Start SeaweedFS
docker run -d --name seaweedfs -p 8333:8333 chrislusf/seaweedfs server -s3 -dir=/data

# Create bucket
docker exec -i seaweedfs sh -c 'echo "s3.bucket.create -name test-bucket" | weed shell'

# Run Flink with SeaweedFS
export FLINK_HOME=/path/to/flink
cat > $FLINK_HOME/conf/config.yaml <<EOF
s3.endpoint: http://localhost:8333
fs.s3.aws.credentials.provider: AnonymousCredentialsProvider
s3.path-style-access: true
s3.chunked-encoding.enabled: false
s3.checksum-validation.enabled: false
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
