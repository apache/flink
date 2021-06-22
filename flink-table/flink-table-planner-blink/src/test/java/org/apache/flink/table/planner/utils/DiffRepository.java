/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.XmlOutput;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// THIS FILE IS COPIED FROM APACHE CALCITE

/**
 * A collection of resources used by tests.
 *
 * <p>Loads files containing test input and output into memory. If there are differences, writes out
 * a log file containing the actual output.
 *
 * <p>Typical usage is as follows. A test case class defines a method
 *
 * <blockquote>
 *
 * <pre><code>
 * package com.acme.test;
 * &nbsp;
 * public class MyTest extends TestCase {
 *   public DiffRepository getDiffRepos() {
 *     return DiffRepository.lookup(MyTest.class);
 *   }
 * &nbsp;
 *   &#64;Test public void testToUpper() {
 *     getDiffRepos().assertEquals("${result}", "${string}");
 *   }
 * &nbsp;
 *   &#64;Test public void testToLower() {
 *     getDiffRepos().assertEquals("Multi-line\nstring", "${string}");
 *   }
 * }
 * </code></pre>
 *
 * </blockquote>
 *
 * <p>There is an accompanying reference file named after the class, <code>
 * src/test/resources/com/acme/test/MyTest.xml</code>:
 *
 * <blockquote>
 *
 * <pre><code>
 * &lt;Root&gt;
 *     &lt;TestCase name="testToUpper"&gt;
 *         &lt;Resource name="string"&gt;
 *             &lt;![CDATA[String to be converted to upper case]]&gt;
 *         &lt;/Resource&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[STRING TO BE CONVERTED TO UPPER CASE]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 *     &lt;TestCase name="testToLower"&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[multi-line
 * string]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 * &lt;/Root&gt;
 *
 * </code></pre>
 *
 * </blockquote>
 *
 * <p>If any of the test cases fails, a log file is generated, called <code>
 * target/surefire/com/acme/test/MyTest.xml</code>, containing the actual output.
 *
 * <p>(Maven sometimes removes this file; if it is not present, run maven with an extra {@code -X}
 * flag. See <a href="http://jira.codehaus.org/browse/SUREFIRE-846">[SUREFIRE-846]</a> for details.)
 *
 * <p>The log file is otherwise identical to the reference log, so once the log file has been
 * verified, it can simply be copied over to become the new reference log:
 *
 * <blockquote>
 *
 * <code>cp target/surefire/com/acme/test/MyTest.xml
 * src/test/resources/com/acme/test/MyTest.xml</code>
 *
 * </blockquote>
 *
 * <p>If a resource or test case does not exist, <code>DiffRepository</code> creates them in the log
 * file. Because DiffRepository is so forgiving, it is very easy to create new tests and test cases.
 *
 * <p>The {@link #lookup} method ensures that all test cases share the same instance of the
 * repository. This is important more than one one test case fails. The shared instance ensures that
 * the generated <code>target/surefire/com/acme/test/MyTest.xml</code> file contains the actual for
 * <em>both</em> test cases.
 */
public class DiffRepository {
    // ~ Static fields/initializers ---------------------------------------------

    /*
          Example XML document:

          <Root>
            <TestCase name="testFoo">
              <Resource name="sql">
                <![CDATA[select from emps]]>
               </Resource>
               <Resource name="plan">
                 <![CDATA[MockTableImplRel.FENNEL_EXEC(table=[SALES, EMP])]]>
               </Resource>
             </TestCase>
             <TestCase name="testBar">
               <Resource name="sql">
                 <![CDATA[select * from depts where deptno = 10]]>
               </Resource>
               <Resource name="output">
                 <![CDATA[10, 'Sales']]>
               </Resource>
             </TestCase>
           </Root>
    */
    private static final String ROOT_TAG = "Root";
    private static final String TEST_CASE_TAG = "TestCase";
    private static final String TEST_CASE_NAME_ATTR = "name";
    private static final String TEST_CASE_OVERRIDES_ATTR = "overrides";
    private static final String RESOURCE_TAG = "Resource";
    private static final String RESOURCE_NAME_ATTR = "name";

    private static final String LICENSE =
            "\n"
                    + "Licensed to the Apache Software Foundation (ASF) under one or more\n"
                    + "contributor license agreements.  See the NOTICE file distributed with\n"
                    + "this work for additional information regarding copyright ownership.\n"
                    + "The ASF licenses this file to you under the Apache License, Version 2.0\n"
                    + "(the \"License\"); you may not use this file except in compliance with\n"
                    + "the License.  You may obtain a copy of the License at\n"
                    + "\n"
                    + "http://www.apache.org/licenses/LICENSE-2.0\n"
                    + "\n"
                    + "Unless required by applicable law or agreed to in writing, software\n"
                    + "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
                    + "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                    + "See the License for the specific language governing permissions and\n"
                    + "limitations under the License.\n";

    /**
     * Holds one diff-repository per class. It is necessary for all test cases in the same class to
     * share the same diff-repository: if the repository gets loaded once per test case, then only
     * one diff is recorded.
     */
    private static final Map<Class, DiffRepository> MAP_CLASS_TO_REPOSITORY = new HashMap<>();

    // ~ Instance fields --------------------------------------------------------

    private final DiffRepository baseRepository;
    private final int indent;
    private Document doc;
    private final Element root;
    private final File logFile;
    private final Filter filter;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a DiffRepository.
     *
     * @param refFile Reference file
     * @param logFile Log file
     * @param baseRepository Parent repository or null
     * @param filter Filter or null
     */
    private DiffRepository(
            URL refFile, File logFile, DiffRepository baseRepository, Filter filter) {
        this.baseRepository = baseRepository;
        this.filter = filter;
        if (refFile == null) {
            throw new IllegalArgumentException("url must not be null");
        }
        this.logFile = logFile;

        // Load the document.
        DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder docBuilder = fac.newDocumentBuilder();
            try {
                // Parse the reference file.
                this.doc = docBuilder.parse(refFile.openStream());
                // Don't write a log file yet -- as far as we know, it's still
                // identical.
            } catch (IOException | SAXParseException e) {
                // There's no reference file. Create and write a log file.
                this.doc = docBuilder.newDocument();
                this.doc.appendChild(doc.createComment(LICENSE));
                this.doc.appendChild(doc.createElement(ROOT_TAG));
                flushDoc();
            }
            this.root = doc.getDocumentElement();
            if (!root.getNodeName().equals(ROOT_TAG)) {
                throw new RuntimeException(
                        "expected root element of type '"
                                + ROOT_TAG
                                + "', but found '"
                                + root.getNodeName()
                                + "'");
            }
        } catch (ParserConfigurationException | SAXException e) {
            throw new RuntimeException("error while creating xml parser", e);
        }
        indent =
                logFile.getPath().contains("RelOptRulesTest")
                                || logFile.getPath().contains("SqlToRelConverterTest")
                                || logFile.getPath().contains("SqlLimitsTest")
                        ? 4
                        : 2;
    }

    // ~ Methods ----------------------------------------------------------------

    private static URL findFile(Class clazz, final String suffix) {
        // The reference file for class "com.foo.Bar" is "com/foo/Bar.xml"
        String rest = "/" + clazz.getName().replace('.', File.separatorChar) + suffix;
        URL url = clazz.getResource(rest);
        if (url == null) {
            File path = new File("./src/test/resources", rest);
            try {
                path.getParentFile().mkdirs();
                path.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Can not create file " + path.getAbsolutePath(), e);
            }
            try {
                url = path.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(
                        "Can not get URL of file path " + path.getAbsolutePath(), e);
            }
        }
        return url;
    }

    /**
     * Expands a string containing one or more variables. (Currently only works if there is one
     * variable.)
     */
    public synchronized String expand(String testCaseName, String tag, String text) {
        if (text == null) {
            return null;
        } else if (text.startsWith("${") && text.endsWith("}")) {
            final String token = text.substring(2, text.length() - 1);
            if (tag == null) {
                tag = token;
            }
            assert token.startsWith(tag) : "token '" + token + "' does not match tag '" + tag + "'";
            String expanded = get(testCaseName, token);
            if (expanded == null) {
                // Token is not specified. Return the original text: this will
                // cause a diff, and the actual value will be written to the
                // log file.
                return text;
            }
            if (filter != null) {
                expanded = filter.filter(this, testCaseName, tag, text, expanded);
            }
            return expanded;
        } else {
            // Make sure what appears in the resource file is consistent with
            // what is in the Java. It helps to have a redundant copy in the
            // resource file.
            if (baseRepository == null || baseRepository.get(testCaseName, tag) == null) {
                set(testCaseName, tag, text);
            }
            return text;
        }
    }

    /**
     * Sets the value of a given resource of the current test case.
     *
     * @param resourceName Name of the resource, e.g. "sql"
     * @param value Value of the resource
     */
    public synchronized void set(String testCaseName, String resourceName, String value) {
        assert resourceName != null;
        update(testCaseName, resourceName, value);
    }

    public void amend(String testCaseName, String expected, String actual) {
        if (expected.startsWith("${") && expected.endsWith("}")) {
            String token = expected.substring(2, expected.length() - 1);
            set(testCaseName, token, actual);
        }
    }

    /**
     * Returns a given resource from a given test case.
     *
     * @param testCaseName Name of test case, e.g. "testFoo"
     * @param resourceName Name of resource, e.g. "sql", "plan"
     * @return The value of the resource, or null if not found
     */
    private synchronized String get(final String testCaseName, String resourceName) {
        Element testCaseElement = getTestCaseElement(testCaseName, true, null);
        if (testCaseElement == null) {
            if (baseRepository != null) {
                return baseRepository.get(testCaseName, resourceName);
            } else {
                return null;
            }
        }
        final Element resourceElement = getResourceElement(testCaseElement, resourceName);
        if (resourceElement != null) {
            return getText(resourceElement);
        }
        return null;
    }

    /** Returns the text under an element. */
    private static String getText(Element element) {
        // If there is a <![CDATA[ ... ]]> child, return its text and ignore
        // all other child elements.
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof CDATASection) {
                return node.getNodeValue();
            }
        }

        // Otherwise return all the text under this element (including
        // whitespace).
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof Text) {
                buf.append(((Text) node).getWholeText());
            }
        }
        return buf.toString();
    }

    /**
     * Returns the &lt;TestCase&gt; element corresponding to the current test case.
     *
     * @param testCaseName Name of test case
     * @param checkOverride Make sure that if an element overrides an element in a base repository,
     *     it has overrides="true"
     * @return TestCase element, or null if not found
     */
    private synchronized Element getTestCaseElement(
            final String testCaseName,
            boolean checkOverride,
            List<Pair<String, Element>> elements) {
        final NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(TEST_CASE_TAG)) {
                Element testCase = (Element) child;
                final String name = testCase.getAttribute(TEST_CASE_NAME_ATTR);
                if (testCaseName.equals(name)) {
                    if (checkOverride
                            && (baseRepository != null)
                            && (baseRepository.getTestCaseElement(testCaseName, false, null)
                                    != null)
                            && !"true".equals(testCase.getAttribute(TEST_CASE_OVERRIDES_ATTR))) {
                        throw new RuntimeException(
                                "TestCase  '"
                                        + testCaseName
                                        + "' overrides a "
                                        + "test case in the base repository, but does "
                                        + "not specify 'overrides=true'");
                    }
                    return testCase;
                }
                if (elements != null) {
                    elements.add(Pair.of(name, testCase));
                }
            }
        }
        return null;
    }

    public void assertEquals(String testCaseName, String tag, String expected, String actual) {
        String expected2 = expand(testCaseName, tag, expected);
        if (expected2 == null) {
            update(testCaseName, expected, actual);
            throw new AssertionError(
                    "reference file does not contain resource '"
                            + expected
                            + "' for test case '"
                            + testCaseName
                            + "'");
        } else {
            try {
                // TODO jvs 25-Apr-2006:  reuse bulk of
                // DiffTestCase.diffTestLog here; besides newline
                // insensitivity, it can report on the line
                // at which the first diff occurs, which is useful
                // for largish snippets
                String expected2Canonical = expected2.replace(Util.LINE_SEPARATOR, "\n");
                String actualCanonical = actual.replace(Util.LINE_SEPARATOR, "\n");
                Assert.assertEquals(tag, expected2Canonical, actualCanonical);
            } catch (ComparisonFailure e) {
                amend(testCaseName, expected, actual);
                throw e;
            }
        }
    }

    /**
     * Creates a new document with a given resource.
     *
     * <p>This method is synchronized, in case two threads are running test cases of this test at
     * the same time.
     *
     * @param testCaseName Test case name
     * @param resourceName Resource name
     * @param value New value of resource
     */
    private synchronized void update(String testCaseName, String resourceName, String value) {
        final List<Pair<String, Element>> map = new ArrayList<>();
        Element testCaseElement = getTestCaseElement(testCaseName, true, map);
        if (testCaseElement == null) {
            testCaseElement = doc.createElement(TEST_CASE_TAG);
            testCaseElement.setAttribute(TEST_CASE_NAME_ATTR, testCaseName);
            Node refElement = ref(testCaseName, map);
            root.insertBefore(testCaseElement, refElement);
        }
        Element resourceElement = getResourceElement(testCaseElement, resourceName, true);
        if (resourceElement == null) {
            resourceElement = doc.createElement(RESOURCE_TAG);
            resourceElement.setAttribute(RESOURCE_NAME_ATTR, resourceName);
            testCaseElement.appendChild(resourceElement);
        } else {
            removeAllChildren(resourceElement);
        }
        if (!value.equals("")) {
            resourceElement.appendChild(doc.createCDATASection(value));
        }

        // Write out the document.
        flushDoc();
    }

    private Node ref(String testCaseName, List<Pair<String, Element>> map) {
        if (map.isEmpty()) {
            return null;
        }
        // Compute the position that the new element should be if the map were
        // sorted.
        int i = 0;
        final List<String> names = Pair.left(map);
        for (String s : names) {
            if (s.compareToIgnoreCase(testCaseName) <= 0) {
                ++i;
            }
        }
        // Starting at a proportional position in the list,
        // move forwards through lesser names, then
        // move backwards through greater names.
        //
        // The intended effect is that if the list is already sorted, the new item
        // will end up in exactly the right position, and if the list is not sorted,
        // the new item will end up in approximately the right position.
        while (i < map.size() && names.get(i).compareToIgnoreCase(testCaseName) < 0) {
            ++i;
        }
        if (i >= map.size() - 1) {
            return null;
        }
        while (i >= 0 && names.get(i).compareToIgnoreCase(testCaseName) > 0) {
            --i;
        }
        return map.get(i + 1).right;
    }

    /** Flushes the reference document to the file system. */
    private void flushDoc() {
        try {
            boolean b = logFile.getParentFile().mkdirs();
            Util.discard(b);
            try (Writer w = Util.printWriter(logFile)) {
                write(doc, w, indent);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "error while writing test reference log '" + logFile + "'", e);
        }
    }

    /**
     * Returns a given resource from a given test case.
     *
     * @param testCaseElement The enclosing TestCase element, e.g. <code>
     *                        &lt;TestCase name="testFoo"&gt;</code>.
     * @param resourceName Name of resource, e.g. "sql", "plan"
     * @return The value of the resource, or null if not found
     */
    private static Element getResourceElement(Element testCaseElement, String resourceName) {
        return getResourceElement(testCaseElement, resourceName, false);
    }

    /**
     * Returns a given resource from a given test case.
     *
     * @param testCaseElement The enclosing TestCase element, e.g. <code>
     *                            &lt;TestCase name="testFoo"&gt;</code>.
     * @param resourceName Name of resource, e.g. "sql", "plan"
     * @param killYoungerSiblings Whether to remove resources with the same name and the same parent
     *     that are eclipsed
     * @return The value of the resource, or null if not found
     */
    private static Element getResourceElement(
            Element testCaseElement, String resourceName, boolean killYoungerSiblings) {

        final NodeList childNodes = testCaseElement.getChildNodes();
        Element found = null;
        final List<Node> kills = new ArrayList<>();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(RESOURCE_TAG)
                    && resourceName.equals(((Element) child).getAttribute(RESOURCE_NAME_ATTR))) {
                if (found == null) {
                    found = (Element) child;
                } else if (killYoungerSiblings) {
                    kills.add(child);
                }
            }
        }
        for (Node kill : kills) {
            testCaseElement.removeChild(kill);
        }
        return found;
    }

    private static void removeAllChildren(Element element) {
        final NodeList childNodes = element.getChildNodes();
        while (childNodes.getLength() > 0) {
            element.removeChild(childNodes.item(0));
        }
    }

    /**
     * Serializes an XML document as text.
     *
     * <p>TODO: I'm sure there's a library call to do this, but I'm danged if I can find it. --
     * jhyde, 2006/2/9.
     */
    private static void write(Document doc, Writer w, int indent) {
        final XmlOutput out = new XmlOutput(w);
        out.setGlob(true);
        out.setIndentString(Spaces.of(indent));
        writeNode(doc, out);
    }

    private static void writeNode(Node node, XmlOutput out) {
        final NodeList childNodes;
        switch (node.getNodeType()) {
            case Node.DOCUMENT_NODE:
                out.print("<?xml version=\"1.0\" ?>\n");
                childNodes = node.getChildNodes();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    Node child = childNodes.item(i);
                    writeNode(child, out);
                }

                //            writeNode(((Document) node).getDocumentElement(),
                // out);
                break;

            case Node.ELEMENT_NODE:
                Element element = (Element) node;
                final String tagName = element.getTagName();
                out.beginBeginTag(tagName);

                // Attributes.
                final NamedNodeMap attributeMap = element.getAttributes();
                for (int i = 0; i < attributeMap.getLength(); i++) {
                    final Node att = attributeMap.item(i);
                    out.attribute(att.getNodeName(), att.getNodeValue());
                }
                out.endBeginTag(tagName);

                // Write child nodes, ignoring attributes but including text.
                childNodes = node.getChildNodes();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    Node child = childNodes.item(i);
                    if (child.getNodeType() == Node.ATTRIBUTE_NODE) {
                        continue;
                    }
                    writeNode(child, out);
                }
                out.endTag(tagName);
                break;

            case Node.ATTRIBUTE_NODE:
                out.attribute(node.getNodeName(), node.getNodeValue());
                break;

            case Node.CDATA_SECTION_NODE:
                CDATASection cdata = (CDATASection) node;
                out.cdata(cdata.getNodeValue(), true);
                break;

            case Node.TEXT_NODE:
                Text text = (Text) node;
                final String wholeText = text.getNodeValue();
                if (!isWhitespace(wholeText)) {
                    out.cdata(wholeText, false);
                }
                break;

            case Node.COMMENT_NODE:
                Comment comment = (Comment) node;
                out.print("<!--" + comment.getNodeValue() + "-->\n");
                break;

            default:
                throw new RuntimeException(
                        "unexpected node type: " + node.getNodeType() + " (" + node + ")");
        }
    }

    private static boolean isWhitespace(String text) {
        for (int i = 0, count = text.length(); i < count; ++i) {
            final char c = text.charAt(i);
            switch (c) {
                case ' ':
                case '\t':
                case '\n':
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

    /**
     * Finds the repository instance for a given class, with no base repository or filter.
     *
     * @param clazz Test case class
     * @return The diff repository shared between test cases in this class.
     */
    public static DiffRepository lookup(Class clazz) {
        return lookup(clazz, null);
    }

    /**
     * Finds the repository instance for a given class and inheriting from a given repository.
     *
     * @param clazz Test case class
     * @param baseRepository Base class of test class
     * @return The diff repository shared between test cases in this class.
     */
    public static DiffRepository lookup(Class clazz, DiffRepository baseRepository) {
        return lookup(clazz, baseRepository, null);
    }

    /**
     * Finds the repository instance for a given class.
     *
     * <p>It is important that all test cases in a class share the same repository instance. This
     * ensures that, if two or more test cases fail, the log file will contains the actual results
     * of both test cases.
     *
     * <p>The <code>baseRepository</code> parameter is useful if the test is an extension to a
     * previous test. If the test class has a base class which also has a repository, specify the
     * repository here. DiffRepository will look for resources in the base class if it cannot find
     * them in this repository. If test resources from test cases in the base class are missing or
     * incorrect, it will not write them to the log file -- you probably need to fix the base test.
     *
     * <p>Use the <code>filter</code> parameter if you expect the test to return results slightly
     * different than in the repository. This happens if the behavior of a derived test is slightly
     * different than a base test. If you do not specify a filter, no filtering will happen.
     *
     * @param clazz Test case class
     * @param baseRepository Base repository
     * @param filter Filters each string returned by the repository
     * @return The diff repository shared between test cases in this class.
     */
    public static synchronized DiffRepository lookup(
            Class clazz, DiffRepository baseRepository, Filter filter) {
        DiffRepository diffRepository = MAP_CLASS_TO_REPOSITORY.get(clazz);
        if (diffRepository == null) {
            final URL refFile = findFile(clazz, ".xml");
            final File logFile = new File(refFile.getFile().replace("test-classes", "surefire"));
            diffRepository = new DiffRepository(refFile, logFile, baseRepository, filter);
            MAP_CLASS_TO_REPOSITORY.put(clazz, diffRepository);
        }
        return diffRepository;
    }

    /** Callback to filter strings before returning them. */
    public interface Filter {
        /**
         * Filters a string.
         *
         * @param diffRepository Repository
         * @param testCaseName Test case name
         * @param tag Tag being expanded
         * @param text Text being expanded
         * @param expanded Expanded text
         * @return Expanded text after filtering
         */
        String filter(
                DiffRepository diffRepository,
                String testCaseName,
                String tag,
                String text,
                String expanded);
    }
}

// End DiffRepository.java
