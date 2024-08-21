package org.apache.flink.table.planner.operations;

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SqlShowToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @BeforeEach
    public void before() throws TableAlreadyExistException, DatabaseNotExistException {
        // Do nothing
        // No need to create schema, tables and etc. since the test executes for unset catalog and
        // database
    }

    @AfterEach
    public void after() throws TableNotExistException {
        // Do nothing
    }

    @ParameterizedTest
    @ValueSource(strings = {"SHOW TABLES", "SHOW VIEWS", "SHOW FUNCTIONS", "SHOW PROCEDURES"})
    void testParseShowFunctionForUnsetCatalog(String sql) {
        catalogManager.setCurrentCatalog(null);
        // No exception should be thrown during parsing.
        // Validation exception should be thrown while execution.
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {"SHOW TABLES", "SHOW VIEWS", "SHOW FUNCTIONS", "SHOW PROCEDURES"})
    void testParseShowFunctionForUnsetDatabase(String sql) {
        catalogManager.setCurrentDatabase(null);
        // No exception should be thrown during parsing.
        // Validation exception should be thrown while execution.
        parse(sql);
    }
}
