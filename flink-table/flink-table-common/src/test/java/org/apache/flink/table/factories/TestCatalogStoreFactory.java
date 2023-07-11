package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestCatalogStoreFactory implements CatalogStoreFactory {

    public static final String IDENTIFIER = "test-catalog-store";

    @Override
    public CatalogStore createCatalogStore(Context context) {
        return new TestCatalogStore(context.getOptions());
    }

    @Override
    public void open(Context context) throws CatalogException {}

    @Override
    public void close() throws CatalogException {}

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    public static class TestCatalogStore implements CatalogStore {
        private final Map<String, String> options;

        public TestCatalogStore(Map<String, String> options) {
            this.options = options;
        }

        @Override
        public void storeCatalog(String catalogName, CatalogDescriptor catalog)
                throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
                throws CatalogException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<CatalogDescriptor> getCatalog(String catalogName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> listCatalogs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(String catalogName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
