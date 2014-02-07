package eu.stratosphere.test.exampleRecordPrograms;

import eu.stratosphere.api.common.operators.CollectionDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the input field validation of CollectionDataSource
 */
public class CollectionValidationTest {
    @Test
    public void TestArrayInputValidation() throws Exception {

        /*
        valid array input
         */
        try {
            CollectionDataSource source = new CollectionDataSource("test_1d_valid_array","a","b","c");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        try {
            CollectionDataSource source = new CollectionDataSource("test_2d_valid_array",new Object[][]{{1,"a"},{2,"b"},{3,"c"}});
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        /*
        invalid array input
         */
        try {
            CollectionDataSource source = new CollectionDataSource("test_1d_invalid_array",1,"b","c");
            Assert.fail("input type is different");
        } catch (Exception e) {
        }

        try {
            CollectionDataSource source = new CollectionDataSource("test_2d_invalid_array",new Object[][]{{1,"a"},{2,"b"},{3,4}});
            Assert.fail("input type is different");
        } catch (Exception e) {
        }

    }

    @Test
    public void TestCollectionInputValidation() throws Exception {
        /*
        	valid collection input
         */
        try {
            List<Object> tmp= new ArrayList<Object>();
            for (int i = 0; i < 100; i++) {
                tmp.add(i);
            }
            CollectionDataSource source = new CollectionDataSource(tmp,"test_valid_collection");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        try {
            List<Object> tmp= new ArrayList<Object>();
            for (int i = 0; i < 100; i++) {
                List<Object> inner = new ArrayList<Object>();
                inner.add(i);
                inner.add('a' + i);
                tmp.add(inner);
            }
            CollectionDataSource source = new CollectionDataSource(tmp,"test_valid_double_collection");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        /*
        invalid collection input
         */
        try {
            List<Object> tmp= new ArrayList<Object>();
            for (int i = 0; i < 100; i++) {
                tmp.add(i);
            }
            tmp.add("a");
            CollectionDataSource source = new CollectionDataSource(tmp,"test_invalid_collection");
            Assert.fail("input type is different");
        } catch (Exception e) {
        }

        try {
            List<Object> tmp= new ArrayList<Object>();
            for (int i = 0; i < 100; i++) {
                List<Object> inner = new ArrayList<Object>();
                inner.add(i);
                inner.add('a' + i);
                tmp.add(inner);
            }
            List<Object> inner = new ArrayList<Object>();
            inner.add('a');
            inner.add('a');
            tmp.add(inner);
            CollectionDataSource source = new CollectionDataSource(tmp, "test_invalid_double_collection");
            Assert.fail("input type is different");
        } catch (Exception e) {
        }
    }
}
