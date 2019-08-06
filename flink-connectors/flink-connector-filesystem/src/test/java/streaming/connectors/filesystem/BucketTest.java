package streaming.connectors.filesystem;

import org.apache.flink.streaming.connectors.fs.table.descriptors.Bucket;
import org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BucketTest extends DescriptorTestBase {

	@Override
	protected List<Descriptor> descriptors() {
		final Descriptor fileRawDesc =
			new Bucket().basePath("file:///tmp/flink-data/json")
				.rawFormat()
				.dateFormat("yyyy-MM-dd-HHmm");

		final Descriptor fileBultDesc =
			new Bucket().basePath("hdfs://localhost/tmp/flink-data/avro")
				.bultFormat()
				.dateFormat("yyyy-MM-dd");

		return Arrays.asList(fileRawDesc, fileBultDesc);
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> fileRawDesc = new HashMap<>();
		fileRawDesc.put("connector.property-version", "1");
		fileRawDesc.put("connector.type", "bucket");
		fileRawDesc.put("connector.basepath", "file:///tmp/flink-data/json");
		fileRawDesc.put("connector.date.format", "yyyy-MM-dd-HHmm");
		fileRawDesc.put("connector.format.type", "raw");

		final Map<String, String> fileBultDesc = new HashMap<>();
		fileBultDesc.put("connector.property-version", "1");
		fileBultDesc.put("connector.type", "bucket");
		fileBultDesc.put("connector.basepath", "hdfs://localhost/tmp/flink-data/avro");
		fileBultDesc.put("connector.date.format", "yyyy-MM-dd");
		fileBultDesc.put("connector.format.type", "bult");

		return Arrays.asList(fileRawDesc, fileBultDesc);
	}

	@Override
	protected DescriptorValidator validator() {
		return new BucketValidator();
	}
}
