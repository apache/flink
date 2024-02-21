package org.apache.flink.table.catalog;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

/**
 * Tests for {@link CatalogManager} and {@link Catalog} which requires proper thread context
 * classloader.
 */
public class CatalogManagerContextClassloaderTest {
    @Test
    public void testOpenCatalogWithContextClassloader() throws MalformedURLException {
        final String catalogJarFile = "target/test-catalog-jar-test-jar.jar";
        CatalogStoreHolder catalogStoreHolder =
                CatalogStoreHolder.newBuilder()
                        .catalogStore(new GenericInMemoryCatalogStore())
                        .config(new Configuration())
                        .classloader(
                                new URLClassLoader(
                                        new URL[] {new File(catalogJarFile).toURI().toURL()},
                                        Thread.currentThread().getContextClassLoader()))
                        .build();
        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(CatalogManagerTest.class.getClassLoader())
                        .config(new Configuration())
                        .defaultCatalog("default", new GenericInMemoryCatalog("default"))
                        .catalogStoreHolder(catalogStoreHolder)
                        .build();
        catalogManager.createCatalog(
                "test",
                CatalogDescriptor.of(
                        "test", Configuration.fromMap(Collections.singletonMap("type", "test"))));
    }
}
