package org.apache.flink.table.format.single;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;

/**
 * Tests for the {@link SingleValue} descriptor.
 */
public class SingleValueTest extends DescriptorTestBase {

	private static final TableSchema tableSchema = TableSchema.builder().build();

	private static final Descriptor DESCRIPTOR_WITH_SCHEMA = new SingleValue();

	@Override
	protected List<Descriptor> descriptors() {
		return Arrays.asList(new SingleValue());
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> props = new HashMap<>();
		props.put("format.type", "single-value");
		props.put("format.property-version", "1");

		return Arrays.asList(props);
	}

	@Override
	protected DescriptorValidator validator() {
		return new SingleValueValidator();
	}
}
