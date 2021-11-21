package org.apache.flink.mongodb.table.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.Factory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FactoryOptionUtil {
    public static Map<String, String> normalizeOptionCaseAsFactory(final Factory factory, final Map<String, String> options) {
        final Map<String, String> normalizedOptions = new HashMap<String, String>();
        final Map<String, String> requiredOptionKeysLowerCaseToOriginal = (Map<String, String>) factory.requiredOptions().stream().collect(Collectors.toMap(option -> option.key().toLowerCase(), ConfigOption::key));
        final Map<String, String> optionalOptionKeysLowerCaseToOriginal = (Map<String, String>) factory.optionalOptions().stream().collect(Collectors.toMap(option -> option.key().toLowerCase(), ConfigOption::key));
        for (final Map.Entry<String, String> entry : options.entrySet()) {
            final String catalogOptionKey = entry.getKey();
            final String catalogOptionValue = entry.getValue();
            normalizedOptions.put(requiredOptionKeysLowerCaseToOriginal.containsKey(catalogOptionKey.toLowerCase()) ? requiredOptionKeysLowerCaseToOriginal.get(catalogOptionKey.toLowerCase()) : optionalOptionKeysLowerCaseToOriginal.getOrDefault(catalogOptionKey.toLowerCase(), catalogOptionKey), catalogOptionValue);
        }
        return normalizedOptions;
    }
}
