################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import warnings
from abc import abstractmethod

from typing import TYPE_CHECKING, Optional

from pyflink.common.serialization import BulkWriterFactory, RowDataBulkWriterFactory

if TYPE_CHECKING:
    from pyflink.table.types import RowType

from pyflink.common import Duration, Encoder
from pyflink.datastream.connectors import Source, Sink
from pyflink.datastream.connectors.base import SupportsPreprocessing, StreamTransformer
from pyflink.datastream.functions import SinkFunction
from pyflink.common.utils import JavaObjectWrapper
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray, is_instance_of

__all__ = [
    'FileCompactor',
    'FileCompactStrategy',
    'OutputFileConfig',
    'FileSource',
    'FileSourceBuilder',
    'FileSink',
    'StreamingFileSink',
    'StreamFormat',
    'BulkFormat',
    'FileEnumeratorProvider',
    'FileSplitAssignerProvider',
    'RollingPolicy',
    'BucketAssigner'
]


# ---- FileSource ----

class FileEnumeratorProvider(object):
    """
    Factory for FileEnumerator which task is to discover all files to be read and to split them
    into a set of file source splits. This includes possibly, path traversals, file filtering
    (by name or other patterns) and deciding whether to split files into multiple splits, and
    how to split them.
    """

    def __init__(self, j_file_enumerator_provider):
        self._j_file_enumerator_provider = j_file_enumerator_provider

    @staticmethod
    def default_splittable_file_enumerator() -> 'FileEnumeratorProvider':
        """
        The default file enumerator used for splittable formats. The enumerator recursively
        enumerates files, split files that consist of multiple distributed storage blocks into
        multiple splits, and filters hidden files (files starting with '.' or '_'). Files with
        suffixes of common compression formats (for example '.gzip', '.bz2', '.xy', '.zip', ...)
        will not be split.
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileEnumeratorProvider(JFileSource.DEFAULT_SPLITTABLE_FILE_ENUMERATOR)

    @staticmethod
    def default_non_splittable_file_enumerator() -> 'FileEnumeratorProvider':
        """
        The default file enumerator used for non-splittable formats. The enumerator recursively
        enumerates files, creates one split for the file, and filters hidden files
        (files starting with '.' or '_').
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileEnumeratorProvider(JFileSource.DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR)


class FileSplitAssignerProvider(object):
    """
    Factory for FileSplitAssigner which is responsible for deciding what split should be
    processed next by which node. It determines split processing order and locality.
    """

    def __init__(self, j_file_split_assigner):
        self._j_file_split_assigner = j_file_split_assigner

    @staticmethod
    def locality_aware_split_assigner() -> 'FileSplitAssignerProvider':
        """
        A FileSplitAssigner that assigns to each host preferably splits that are local, before
        assigning splits that are not local.
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileSplitAssignerProvider(JFileSource.DEFAULT_SPLIT_ASSIGNER)


class StreamFormat(object):
    """
    A reader format that reads individual records from a stream.

    Compared to the :class:`~BulkFormat`, the stream format handles a few things out-of-the-box,
    like deciding how to batch records or dealing with compression.

    Internally in the file source, the readers pass batches of records from the reading threads
    (that perform the typically blocking I/O operations) to the async mailbox threads that do
    the streaming and batch data processing. Passing records in batches
    (rather than one-at-a-time) much reduces the thread-to-thread handover overhead.

    This batching is by default based on I/O fetch size for the StreamFormat, meaning the
    set of records derived from one I/O buffer will be handed over as one. See config option
    `source.file.stream.io-fetch-size` to configure that fetch size.
    """

    def __init__(self, j_stream_format):
        self._j_stream_format = j_stream_format

    @staticmethod
    def text_line_format(charset_name: str = "UTF-8") -> 'StreamFormat':
        """
        Creates a reader format that text lines from a file.

        The reader uses Java's built-in java.io.InputStreamReader to decode the byte stream
        using various supported charset encodings.

        This format does not support optimized recovery from checkpoints. On recovery, it will
        re-read and discard the number of lined that were processed before the last checkpoint.
        That is due to the fact that the offsets of lines in the file cannot be tracked through
        the charset decoders with their internal buffering of stream input and charset decoder
        state.

        :param charset_name: The charset to decode the byte stream.
        """
        j_stream_format = get_gateway().jvm.org.apache.flink.connector.file.src.reader. \
            TextLineInputFormat(charset_name)
        return StreamFormat(j_stream_format)


class BulkFormat(object):
    """
    The BulkFormat reads and decodes batches of records at a time. Examples of bulk formats are
    formats like ORC or Parquet.

    Internally in the file source, the readers pass batches of records from the reading threads
    (that perform the typically blocking I/O operations) to the async mailbox threads that do the
    streaming and batch data processing. Passing records in batches (rather than one-at-a-time) much
    reduce the thread-to-thread handover overhead.

    For the BulkFormat, one batch is handed over as one.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_bulk_format):
        self._j_bulk_format = j_bulk_format


class FileSourceBuilder(object):
    """
    The builder for the :class:`~FileSource`, to configure the various behaviors.

    Start building the source via one of the following methods:

        - :func:`~FileSource.for_record_stream_format`
    """

    def __init__(self, j_file_source_builder):
        self._j_file_source_builder = j_file_source_builder

    def monitor_continuously(
            self,
            discovery_interval: Duration) -> 'FileSourceBuilder':
        """
        Sets this source to streaming ("continuous monitoring") mode.

        This makes the source a "continuous streaming" source that keeps running, monitoring
        for new files, and reads these files when they appear and are discovered by the
        monitoring.

        The interval in which the source checks for new files is the discovery_interval. Shorter
        intervals mean that files are discovered more quickly, but also imply more frequent
        listing or directory traversal of the file system / object store.
        """
        self._j_file_source_builder.monitorContinuously(discovery_interval._j_duration)
        return self

    def process_static_file_set(self) -> 'FileSourceBuilder':
        """
        Sets this source to bounded (batch) mode.

        In this mode, the source processes the files that are under the given paths when the
        application is started. Once all files are processed, the source will finish.

        This setting is also the default behavior. This method is mainly here to "switch back"
        to bounded (batch) mode, or to make it explicit in the source construction.
        """
        self._j_file_source_builder.processStaticFileSet()
        return self

    def set_file_enumerator(
            self,
            file_enumerator: 'FileEnumeratorProvider') -> 'FileSourceBuilder':
        """
        Configures the FileEnumerator for the source. The File Enumerator is responsible
        for selecting from the input path the set of files that should be processed (and which
        to filter out). Furthermore, the File Enumerator may split the files further into
        sub-regions, to enable parallelization beyond the number of files.
        """
        self._j_file_source_builder.setFileEnumerator(
            file_enumerator._j_file_enumerator_provider)
        return self

    def set_split_assigner(
            self,
            split_assigner: 'FileSplitAssignerProvider') -> 'FileSourceBuilder':
        """
        Configures the FileSplitAssigner for the source. The File Split Assigner
        determines which parallel reader instance gets which {@link FileSourceSplit}, and in
        which order these splits are assigned.
        """
        self._j_file_source_builder.setSplitAssigner(split_assigner._j_file_split_assigner)
        return self

    def build(self) -> 'FileSource':
        """
        Creates the file source with the settings applied to this builder.
        """
        return FileSource(self._j_file_source_builder.build())


class FileSource(Source):
    """
    A unified data source that reads files - both in batch and in streaming mode.

    This source supports all (distributed) file systems and object stores that can be accessed via
    the Flink's FileSystem class.

    Start building a file source via one of the following calls:

        - :func:`~FileSource.for_record_stream_format`

    This creates a :class:`~FileSource.FileSourceBuilder` on which you can configure all the
    properties of the file source.

    <h2>Batch and Streaming</h2>

    This source supports both bounded/batch and continuous/streaming data inputs. For the
    bounded/batch case, the file source processes all files under the given path(s). In the
    continuous/streaming case, the source periodically checks the paths for new files and will start
    reading those.

    When you start creating a file source (via the
    :class:`~FileSource.FileSourceBuilder` created through one of the above-mentioned methods)
    the source is by default in bounded/batch mode. Call
    :func:`~FileSource.FileSourceBuilder.monitor_continuously` to put the source into continuous
    streaming mode.

    <h2>Format Types</h2>

    The reading of each file happens through file readers defined by <i>file formats</i>. These
    define the parsing logic for the contents of the file. There are multiple classes that the
    source supports. Their interfaces trade of simplicity of implementation and
    flexibility/efficiency.

        - A :class:`~FileSource.StreamFormat` reads the contents of a file from a file stream.
          It is the simplest format to implement, and provides many features out-of-the-box
          (like checkpointing logic) but is limited in the optimizations it
          can apply (such as object reuse, batching, etc.).

    <h2>Discovering / Enumerating Files</h2>

    The way that the source lists the files to be processes is defined by the
    :class:`~FileSource.FileEnumeratorProvider`. The FileEnumeratorProvider is responsible to
    select the relevant files (for example filter out hidden files) and to optionally splits files
    into multiple regions (= file source splits) that can be read in parallel).
    """

    def __init__(self, j_file_source):
        super(FileSource, self).__init__(source=j_file_source)

    @staticmethod
    def for_record_stream_format(stream_format: StreamFormat, *paths: str) -> FileSourceBuilder:
        """
        Builds a new FileSource using a :class:`~FileSource.StreamFormat` to read record-by-record
        from a file stream.

        When possible, stream-based formats are generally easier (preferable) to file-based
        formats, because they support better default behavior around I/O batching or progress
        tracking (checkpoints).

        Stream formats also automatically de-compress files based on the file extension. This
        supports files ending in ".deflate" (Deflate), ".xz" (XZ), ".bz2" (BZip2), ".gz", ".gzip"
        (GZip).
        """
        JPath = get_gateway().jvm.org.apache.flink.core.fs.Path
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        j_paths = to_jarray(JPath, [JPath(p) for p in paths])
        return FileSourceBuilder(
            JFileSource.forRecordStreamFormat(stream_format._j_stream_format, j_paths))

    @staticmethod
    def for_bulk_file_format(bulk_format: BulkFormat, *paths: str) -> FileSourceBuilder:
        JPath = get_gateway().jvm.org.apache.flink.core.fs.Path
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        j_paths = to_jarray(JPath, [JPath(p) for p in paths])
        return FileSourceBuilder(
            JFileSource.forBulkFileFormat(bulk_format._j_bulk_format, j_paths))


# ---- FileSink ----


class BucketAssigner(JavaObjectWrapper):
    """
    A BucketAssigner is used with a file sink to determine the bucket each incoming element should
    be put into.

    The StreamingFileSink can be writing to many buckets at a time, and it is responsible
    for managing a set of active buckets. Whenever a new element arrives it will ask the
    BucketAssigner for the bucket the element should fall in. The BucketAssigner can, for
    example, determine buckets based on system time.
    """

    def __init__(self, j_bucket_assigner):
        super().__init__(j_bucket_assigner)

    @staticmethod
    def base_path_bucket_assigner() -> 'BucketAssigner':
        """
        Creates a BucketAssigner that does not perform any bucketing of files. All files are
        written to the base path.
        """
        return BucketAssigner(get_gateway().jvm.org.apache.flink.streaming.api.functions.sink.
                              filesystem.bucketassigners.BasePathBucketAssigner())

    @staticmethod
    def date_time_bucket_assigner(format_str: str = "yyyy-MM-dd--HH", timezone_id: str = None):
        """
        Creates a BucketAssigner that assigns to buckets based on current system time.

        It will create directories of the following form: /{basePath}/{dateTimePath}/}.
        The basePath is the path that was specified as a base path when creating the new bucket.
        The dateTimePath is determined based on the current system time and the user provided format
        string.

        The Java DateTimeFormatter is used to derive a date string from the current system time and
        the date format string. The default format string is "yyyy-MM-dd--HH" so the rolling files
        will have a granularity of hours.

        :param format_str: The format string used to determine the bucket id.
        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-08:00". Th e default time zone will b used if it's None.
        """
        if timezone_id is not None and isinstance(timezone_id, str):
            j_timezone = get_gateway().jvm.java.time.ZoneId.of(timezone_id)
        else:
            j_timezone = get_gateway().jvm.java.time.ZoneId.systemDefault()
        return BucketAssigner(
            get_gateway().jvm.org.apache.flink.streaming.api.functions.sink.
            filesystem.bucketassigners.DateTimeBucketAssigner(format_str, j_timezone))


class RollingPolicy(JavaObjectWrapper):
    """
    The policy based on which a Bucket in the FileSink rolls its currently
    open part file and opens a new one.
    """

    def __init__(self, j_rolling_policy):
        super().__init__(j_rolling_policy)

    @staticmethod
    def default_rolling_policy(
            part_size: int = 1024 * 1024 * 128,
            rollover_interval: int = 60 * 1000,
            inactivity_interval: int = 60 * 1000) -> 'DefaultRollingPolicy':
        """
        Returns the default implementation of the RollingPolicy.

        This policy rolls a part file if:

            - there is no open part file,
            - the current file has reached the maximum bucket size (by default 128MB),
            - the current file is older than the roll over interval (by default 60 sec), or
            - the current file has not been written to for more than the allowed inactivityTime (by
              default 60 sec).

        :param part_size: The maximum part file size before rolling.
        :param rollover_interval: The maximum time duration a part file can stay open before
                                  rolling.
        :param inactivity_interval: The time duration of allowed inactivity after which a part file
                                    will have to roll.
        """
        JDefaultRollingPolicy = get_gateway().jvm.org.apache.flink.streaming.api.functions.\
            sink.filesystem.rollingpolicies.DefaultRollingPolicy
        j_rolling_policy = JDefaultRollingPolicy.builder()\
            .withMaxPartSize(part_size) \
            .withRolloverInterval(rollover_interval) \
            .withInactivityInterval(inactivity_interval) \
            .build()
        return DefaultRollingPolicy(j_rolling_policy)

    @staticmethod
    def on_checkpoint_rolling_policy() -> 'OnCheckpointRollingPolicy':
        """
        Returns a RollingPolicy which rolls (ONLY) on every checkpoint.
        """
        JOnCheckpointRollingPolicy = get_gateway().jvm.org.apache.flink.streaming.api.functions. \
            sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
        return OnCheckpointRollingPolicy(JOnCheckpointRollingPolicy.build())


class DefaultRollingPolicy(RollingPolicy):
    """
    The default implementation of the RollingPolicy.

    This policy rolls a part file if:

        - there is no open part file,
        - the current file has reached the maximum bucket size (by default 128MB),
        - the current file is older than the roll over interval (by default 60 sec), or
        - the current file has not been written to for more than the allowed inactivityTime (by
          default 60 sec).
    """

    def __init__(self, j_rolling_policy):
        super().__init__(j_rolling_policy)


class OnCheckpointRollingPolicy(RollingPolicy):
    """
    A RollingPolicy which rolls (ONLY) on every checkpoint.
    """

    def __init__(self, j_rolling_policy):
        super().__init__(j_rolling_policy)


class OutputFileConfig(JavaObjectWrapper):
    """
    Part file name configuration.
    This allow to define a prefix and a suffix to the part file name.
    """

    @staticmethod
    def builder():
        return OutputFileConfig.OutputFileConfigBuilder()

    def __init__(self, part_prefix: str, part_suffix: str):
        filesystem = get_gateway().jvm.org.apache.flink.streaming.api.functions.sink.filesystem
        self._j_output_file_config = filesystem.OutputFileConfig(part_prefix, part_suffix)
        super().__init__(self._j_output_file_config)

    def get_part_prefix(self) -> str:
        """
        The prefix for the part name.
        """
        return self._j_output_file_config.getPartPrefix()

    def get_part_suffix(self) -> str:
        """
        The suffix for the part name.
        """
        return self._j_output_file_config.getPartSuffix()

    class OutputFileConfigBuilder(object):
        """
        A builder to create the part file configuration.
        """

        def __init__(self):
            self.part_prefix = "part"
            self.part_suffix = ""

        def with_part_prefix(self, prefix) -> 'OutputFileConfig.OutputFileConfigBuilder':
            self.part_prefix = prefix
            return self

        def with_part_suffix(self, suffix) -> 'OutputFileConfig.OutputFileConfigBuilder':
            self.part_suffix = suffix
            return self

        def build(self) -> 'OutputFileConfig':
            return OutputFileConfig(self.part_prefix, self.part_suffix)


class FileCompactStrategy(JavaObjectWrapper):
    """
    Strategy for compacting the files written in {@link FileSink} before committing.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_file_compact_strategy):
        super().__init__(j_file_compact_strategy)

    @staticmethod
    def builder() -> 'FileCompactStrategy.Builder':
        return FileCompactStrategy.Builder()

    class Builder(object):

        def __init__(self):
            JFileCompactStrategy = get_gateway().jvm.org.apache.flink.connector.file.sink.\
                compactor.FileCompactStrategy
            self._j_builder = JFileCompactStrategy.Builder.newBuilder()

        def build(self) -> 'FileCompactStrategy':
            return FileCompactStrategy(self._j_builder.build())

        def enable_compaction_on_checkpoint(self, num_checkpoints_before_compaction: int) \
                -> 'FileCompactStrategy.Builder':
            """
            Optional, compaction will be triggered when N checkpoints passed since the last
            triggering, -1 by default indicating no compaction on checkpoint.
            """
            self._j_builder.enableCompactionOnCheckpoint(num_checkpoints_before_compaction)
            return self

        def set_size_threshold(self, size_threshold: int) -> 'FileCompactStrategy.Builder':
            """
            Optional, compaction will be triggered when the total size of compacting files reaches
            the threshold. -1 by default, indicating the size is unlimited.
            """
            self._j_builder.setSizeThreshold(size_threshold)
            return self

        def set_num_compact_threads(self, num_compact_threads: int) \
                -> 'FileCompactStrategy.Builder':
            """
            Optional, the count of compacting threads in a compactor operator, 1 by default.
            """
            self._j_builder.setNumCompactThreads(num_compact_threads)
            return self


class FileCompactor(JavaObjectWrapper):
    """
    The FileCompactor is responsible for compacting files into one file.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_file_compactor):
        super().__init__(j_file_compactor)

    @staticmethod
    def concat_file_compactor(file_delimiter: bytes = None):
        """
        Returns a file compactor that simply concat the compacting files. The file_delimiter will be
        added between neighbouring files if provided.
        """
        JConcatFileCompactor = get_gateway().jvm.org.apache.flink.connector.file.sink.compactor.\
            ConcatFileCompactor
        if file_delimiter:
            return FileCompactor(JConcatFileCompactor(file_delimiter))
        else:
            return FileCompactor(JConcatFileCompactor())

    @staticmethod
    def identical_file_compactor():
        """
        Returns a file compactor that directly copy the content of the only input file to the
        output.
        """
        JIdenticalFileCompactor = get_gateway().jvm.org.apache.flink.connector.file.sink.compactor.\
            IdenticalFileCompactor
        return FileCompactor(JIdenticalFileCompactor())


class FileSink(Sink, SupportsPreprocessing):
    """
    A unified sink that emits its input elements to FileSystem files within buckets. This
    sink achieves exactly-once semantics for both BATCH and STREAMING.

    When creating the sink a basePath must be specified. The base directory contains one
    directory for every bucket. The bucket directories themselves contain several part files, with
    at least one for each parallel subtask of the sink which is writing data to that bucket.
    These part files contain the actual output data.

    The sink uses a BucketAssigner to determine in which bucket directory each element
    should be written to inside the base directory. The BucketAssigner can, for example, roll
    on every checkpoint or use time or a property of the element to determine the bucket directory.
    The default BucketAssigner is a DateTimeBucketAssigner which will create one new
    bucket every hour. You can specify a custom BucketAssigner using the
    :func:`~FileSink.RowFormatBuilder.with_bucket_assigner`, after calling
    :class:`~FileSink.for_row_format`.

    The names of the part files could be defined using OutputFileConfig. This
    configuration contains a part prefix and a part suffix that will be used with a random uid
    assigned to each subtask of the sink and a rolling counter to determine the file names. For
    example with a prefix "prefix" and a suffix ".ext", a file named {@code
    "prefix-81fc4980-a6af-41c8-9937-9939408a734b-17.ext"} contains the data from subtask with uid
    {@code 81fc4980-a6af-41c8-9937-9939408a734b} of the sink and is the {@code 17th} part-file
    created by that subtask.

    Part files roll based on the user-specified RollingPolicy. By default, a DefaultRollingPolicy
    is used for row-encoded sink output; a OnCheckpointRollingPolicy is
    used for bulk-encoded sink output.

    In some scenarios, the open buckets are required to change based on time. In these cases, the
    user can specify a bucket_check_interval (by default 1m) and the sink will check
    periodically and roll the part file if the specified rolling policy says so.

    Part files can be in one of three states: in-progress, pending or finished. The reason for this
    is how the sink works to provide exactly-once semantics and fault-tolerance. The part file that
    is currently being written to is in-progress. Once a part file is closed for writing it becomes
    pending. When a checkpoint is successful (for STREAMING) or at the end of the job (for BATCH)
    the currently pending files will be moved to finished.

    For STREAMING in order to guarantee exactly-once semantics in case of a failure, the
    sink should roll back to the state it had when that last successful checkpoint occurred. To this
    end, when restoring, the restored files in pending state are transferred into the finished state
    while any in-progress files are rolled back, so that they do not contain data that arrived after
    the checkpoint from which we restore.
    """

    def __init__(self, j_file_sink, transformer: Optional[StreamTransformer] = None):
        super(FileSink, self).__init__(sink=j_file_sink)
        self._transformer = transformer

    def get_transformer(self) -> Optional[StreamTransformer]:
        return self._transformer

    class BaseBuilder(object):

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def with_bucket_check_interval(self, interval: int):
            """
            :param interval: The check interval in milliseconds.
            """
            self._j_builder.withBucketCheckInterval(interval)
            return self

        def with_bucket_assigner(self, bucket_assigner: BucketAssigner):
            self._j_builder.withBucketAssigner(bucket_assigner.get_java_object())
            return self

        def with_output_file_config(self, output_file_config: OutputFileConfig):
            self._j_builder.withOutputFileConfig(output_file_config.get_java_object())
            return self

        def enable_compact(self, strategy: FileCompactStrategy, compactor: FileCompactor):
            self._j_builder.enableCompact(strategy.get_java_object(), compactor.get_java_object())
            return self

        def disable_compact(self):
            self._j_builder.disableCompact()
            return self

        @abstractmethod
        def with_rolling_policy(self, rolling_policy):
            pass

        def build(self):
            return FileSink(self._j_builder.build())

    class RowFormatBuilder(BaseBuilder):
        """
        Builder for the vanilla FileSink using a row format.

        .. versionchanged:: 1.16.0
           Support compaction.
        """

        def __init__(self, j_row_format_builder):
            super().__init__(j_row_format_builder)

        def with_rolling_policy(self, rolling_policy: RollingPolicy):
            self._j_builder.withRollingPolicy(rolling_policy.get_java_object())
            return self

    @staticmethod
    def for_row_format(base_path: str, encoder: Encoder) -> 'FileSink.RowFormatBuilder':
        JPath = get_gateway().jvm.org.apache.flink.core.fs.Path
        JFileSink = get_gateway().jvm.org.apache.flink.connector.file.sink.FileSink

        return FileSink.RowFormatBuilder(
            JFileSink.forRowFormat(JPath(base_path), encoder._j_encoder))

    class BulkFormatBuilder(BaseBuilder):
        """
        Builder for the vanilla FileSink using a bulk format.

        .. versionadded:: 1.16.0
        """

        def __init__(self, j_bulk_format_builder):
            super().__init__(j_bulk_format_builder)
            self._transformer = None

        def with_rolling_policy(self, rolling_policy: OnCheckpointRollingPolicy):
            if not isinstance(rolling_policy, OnCheckpointRollingPolicy):
                raise ValueError('rolling_policy must be OnCheckpointRollingPolicy for bulk format')
            return self

        def _with_row_type(self, row_type: 'RowType') -> 'FileSink.BulkFormatBuilder':
            from pyflink.datastream.data_stream import DataStream
            from pyflink.table.types import _to_java_data_type

            def _check_if_row_data_type(ds) -> bool:
                j_type_info = ds._j_data_stream.getType()
                if not is_instance_of(
                    j_type_info,
                    'org.apache.flink.table.runtime.typeutils.InternalTypeInfo'
                ):
                    return False
                return is_instance_of(
                    j_type_info.toLogicalType(),
                    'org.apache.flink.table.types.logical.RowType'
                )

            class RowRowTransformer(StreamTransformer):

                def apply(self, ds):
                    jvm = get_gateway().jvm

                    if _check_if_row_data_type(ds):
                        return ds

                    j_map_function = jvm.org.apache.flink.python.util.PythonConnectorUtils \
                        .RowRowMapper(_to_java_data_type(row_type))
                    return DataStream(ds._j_data_stream.process(j_map_function))

            self._transformer = RowRowTransformer()
            return self

        def build(self) -> 'FileSink':
            return FileSink(self._j_builder.build(), self._transformer)

    @staticmethod
    def for_bulk_format(base_path: str, writer_factory: BulkWriterFactory) \
            -> 'FileSink.BulkFormatBuilder':
        jvm = get_gateway().jvm
        j_path = jvm.org.apache.flink.core.fs.Path(base_path)
        JFileSink = jvm.org.apache.flink.connector.file.sink.FileSink

        builder = FileSink.BulkFormatBuilder(
            JFileSink.forBulkFormat(j_path, writer_factory.get_java_object())
        )
        if isinstance(writer_factory, RowDataBulkWriterFactory):
            return builder._with_row_type(writer_factory.get_row_type())
        else:
            return builder


# ---- StreamingFileSink ----


class StreamingFileSink(SinkFunction):
    """
    Sink that emits its input elements to `FileSystem` files within buckets. This is
    integrated with the checkpointing mechanism to provide exactly once semantics.


    When creating the sink a `basePath` must be specified. The base directory contains
    one directory for every bucket. The bucket directories themselves contain several part files,
    with at least one for each parallel subtask of the sink which is writing data to that bucket.
    These part files contain the actual output data.
    """

    def __init__(self, j_obj):
        warnings.warn("Deprecated in 1.15. Use FileSink instead.", DeprecationWarning)
        super(StreamingFileSink, self).__init__(j_obj)

    class BaseBuilder(object):

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def with_bucket_check_interval(self, interval: int):
            self._j_builder.withBucketCheckInterval(interval)
            return self

        def with_bucket_assigner(self, bucket_assigner: BucketAssigner):
            self._j_builder.withBucketAssigner(bucket_assigner.get_java_object())
            return self

        @abstractmethod
        def with_rolling_policy(self, policy):
            pass

        def with_output_file_config(self, output_file_config: OutputFileConfig):
            self._j_builder.withOutputFileConfig(output_file_config.get_java_object())
            return self

        def build(self) -> 'StreamingFileSink':
            j_stream_file_sink = self._j_builder.build()
            return StreamingFileSink(j_stream_file_sink)

    class DefaultRowFormatBuilder(BaseBuilder):
        """
        Builder for the vanilla `StreamingFileSink` using a row format.
        """

        def __init__(self, j_default_row_format_builder):
            super().__init__(j_default_row_format_builder)

        def with_rolling_policy(self, policy: RollingPolicy):
            self._j_builder.withRollingPolicy(policy.get_java_object())
            return self

    @staticmethod
    def for_row_format(base_path: str, encoder: Encoder) -> 'DefaultRowFormatBuilder':
        j_path = get_gateway().jvm.org.apache.flink.core.fs.Path(base_path)
        j_default_row_format_builder = get_gateway().jvm.org.apache.flink.streaming.api.\
            functions.sink.filesystem.StreamingFileSink.forRowFormat(j_path, encoder._j_encoder)
        return StreamingFileSink.DefaultRowFormatBuilder(j_default_row_format_builder)

    class DefaultBulkFormatBuilder(BaseBuilder):

        def __init__(self, j_default_bulk_format_builder):
            super().__init__(j_default_bulk_format_builder)

        def with_rolling_policy(self, policy: OnCheckpointRollingPolicy):
            self._j_builder.withRollingPolicy(policy.get_java_object())
            return self

    @staticmethod
    def for_bulk_format(base_path: str, writer_factory: BulkWriterFactory):
        jvm = get_gateway().jvm
        j_path = jvm.org.apache.flink.core.fs.Path(base_path)
        j_default_bulk_format_builder = jvm.org.apache.flink.streaming.api.functions.sink \
            .filesystem.StreamingFileSink.forBulkFormat(j_path, writer_factory.get_java_object())
        return StreamingFileSink.DefaultBulkFormatBuilder(j_default_bulk_format_builder)
