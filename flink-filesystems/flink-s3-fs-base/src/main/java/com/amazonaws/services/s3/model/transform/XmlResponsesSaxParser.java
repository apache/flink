/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Portions copyright 2006-2009 James Murty. Please see LICENSE.txt
 * for applicable license terms and NOTICE.txt for applicable notices.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.s3.model.transform;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.internal.DeleteObjectsResponse;
import com.amazonaws.services.s3.internal.ObjectExpirationResult;
import com.amazonaws.services.s3.internal.S3RequesterChargedResult;
import com.amazonaws.services.s3.internal.S3VersionResult;
import com.amazonaws.services.s3.internal.ServerSideEncryptionResult;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.NoncurrentVersionTransition;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.RequestPaymentConfiguration.Payer;
import com.amazonaws.services.s3.model.analytics.AnalyticsAndOperator;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.analytics.AnalyticsExportDestination;
import com.amazonaws.services.s3.model.analytics.AnalyticsFilter;
import com.amazonaws.services.s3.model.analytics.AnalyticsFilterPredicate;
import com.amazonaws.services.s3.model.analytics.AnalyticsPrefixPredicate;
import com.amazonaws.services.s3.model.analytics.AnalyticsS3BucketDestination;
import com.amazonaws.services.s3.model.analytics.AnalyticsTagPredicate;
import com.amazonaws.services.s3.model.analytics.StorageClassAnalysis;
import com.amazonaws.services.s3.model.analytics.StorageClassAnalysisDataExport;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryDestination;
import com.amazonaws.services.s3.model.inventory.InventoryFilter;
import com.amazonaws.services.s3.model.inventory.InventoryPrefixPredicate;
import com.amazonaws.services.s3.model.inventory.InventoryS3BucketDestination;
import com.amazonaws.services.s3.model.inventory.InventorySchedule;
import com.amazonaws.services.s3.model.inventory.ServerSideEncryptionKMS;
import com.amazonaws.services.s3.model.inventory.ServerSideEncryptionS3;
import com.amazonaws.services.s3.model.lifecycle.LifecycleAndOperator;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilterPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsAndOperator;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsFilter;
import com.amazonaws.services.s3.model.metrics.MetricsFilterPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsPrefixPredicate;
import com.amazonaws.services.s3.model.metrics.MetricsTagPredicate;
import com.amazonaws.util.DateUtils;
import com.amazonaws.util.SdkHttpUtils;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.amazonaws.util.StringUtils.UTF8;

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
//
//  This class is copied from aws-java-sdk-s3 (Apache License 2.0)
//  in order to replaced {@code XMLReaderFactory.createXMLReader} by
//  {@code SAX_PARSER_FACTORY.newSAXParser().getXMLReader()} to
//  avoid JDK-8015099.
//
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

/**
 * XML Sax parser to read XML documents returned by S3 via the REST interface, converting these
 * documents into objects.
 */
public class XmlResponsesSaxParser {
    private static final Log log = LogFactory.getLog(XmlResponsesSaxParser.class);

    private static final SAXParserFactory SAX_PARSER_FACTORY = SAXParserFactory.newInstance();

    private XMLReader xr = null;

    private boolean sanitizeXmlDocument = true;

    /**
     * Constructs the XML SAX parser.
     *
     * @throws SdkClientException
     */
    public XmlResponsesSaxParser() throws SdkClientException {
        // Ensure we can load the XML Reader.
        try {
            // Need to explicitly enable Sax' NAMESPACES_FEATURE (default true) in JAXP (default
            // false)!
            SAX_PARSER_FACTORY.setNamespaceAware(true);
            xr = SAX_PARSER_FACTORY.newSAXParser().getXMLReader();
            disableExternalResourceFetching(xr);
        } catch (SAXException | ParserConfigurationException e) {
            throw new SdkClientException(
                    "Couldn't initialize a SAX driver to create an XMLReader", e);
        }
    }

    /**
     * Parses an XML document from an input stream using a document handler.
     *
     * @param handler the handler for the XML document
     * @param inputStream an input stream containing the XML document to parse
     * @throws IOException on error reading from the input stream (ie connection reset)
     * @throws SdkClientException on error with malformed XML, etc
     */
    protected void parseXmlInputStream(DefaultHandler handler, InputStream inputStream)
            throws IOException {
        try {

            if (log.isDebugEnabled()) {
                log.debug("Parsing XML response document with handler: " + handler.getClass());
            }

            BufferedReader breader =
                    new BufferedReader(
                            new InputStreamReader(inputStream, Constants.DEFAULT_ENCODING));
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);
            xr.parse(new InputSource(breader));

        } catch (IOException e) {
            throw e;

        } catch (Throwable t) {
            try {
                inputStream.close();
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Unable to close response InputStream up after XML parse failure", e);
                }
            }
            throw new SdkClientException(
                    "Failed to parse XML document with handler " + handler.getClass(), t);
        }
    }

    protected InputStream sanitizeXmlDocument(DefaultHandler handler, InputStream inputStream)
            throws IOException {

        if (!sanitizeXmlDocument) {
            // No sanitizing will be performed, return the original input stream unchanged.
            return inputStream;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Sanitizing XML document destined for handler " + handler.getClass());
            }

            InputStream sanitizedInputStream = null;

            try {

                /*
                 * Read object listing XML document from input stream provided into a
                 * string buffer, so we can replace troublesome characters before
                 * sending the document to the XML parser.
                 */
                StringBuilder listingDocBuffer = new StringBuilder();
                BufferedReader br =
                        new BufferedReader(
                                new InputStreamReader(inputStream, Constants.DEFAULT_ENCODING));

                char[] buf = new char[8192];
                int read = -1;
                while ((read = br.read(buf)) != -1) {
                    listingDocBuffer.append(buf, 0, read);
                }
                br.close();

                /*
                 * Replace any carriage return (\r) characters with explicit XML
                 * character entities, to prevent the SAX parser from
                 * misinterpreting 0x0D characters as 0x0A and being unable to
                 * parse the XML.
                 */
                String listingDoc = listingDocBuffer.toString().replaceAll("\r", "&#013;");

                sanitizedInputStream = new ByteArrayInputStream(listingDoc.getBytes(UTF8));

            } catch (IOException e) {
                throw e;

            } catch (Throwable t) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    if (log.isErrorEnabled()) {
                        log.error(
                                "Unable to close response InputStream after failure sanitizing XML document",
                                e);
                    }
                }
                throw new SdkClientException(
                        "Failed to sanitize XML document destined for handler "
                                + handler.getClass(),
                        t);
            }
            return sanitizedInputStream;
        }
    }

    /**
     * Disables certain dangerous features that attempt to automatically fetch DTDs
     *
     * <p>See <a
     * href="https://www.owasp.org/index.php/XML_External_Entity_(XXE)_Prevention_Cheat_Sheet#XMLReader">OWASP
     * XXE Cheat Sheet</a>
     *
     * @param reader the reader to disable the features on
     * @throws SAXNotRecognizedException
     * @throws SAXNotSupportedException
     */
    private void disableExternalResourceFetching(XMLReader reader)
            throws SAXNotRecognizedException, SAXNotSupportedException {
        reader.setFeature("http://xml.org/sax/features/external-general-entities", false);
        reader.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    }

    /**
     * Checks if the specified string is empty or null and if so, returns null. Otherwise simply
     * returns the string.
     *
     * @param s The string to check.
     * @return Null if the specified string was null, or empty, otherwise returns the string the
     *     caller passed in.
     */
    private static String checkForEmptyString(String s) {
        if (s == null) return null;
        if (s.length() == 0) return null;

        return s;
    }

    /**
     * Safely parses the specified string as an integer and returns the value. If a
     * NumberFormatException occurs while parsing the integer, an error is logged and -1 is
     * returned.
     *
     * @param s The string to parse and return as an integer.
     * @return The integer value of the specified string, otherwise -1 if there were any problems
     *     parsing the string as an integer.
     */
    private static int parseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException nfe) {
            log.error("Unable to parse integer value '" + s + "'", nfe);
        }

        return -1;
    }

    /**
     * Safely parses the specified string as a long and returns the value. If a
     * NumberFormatException occurs while parsing the long, an error is logged and -1 is returned.
     *
     * @param s The string to parse and return as a long.
     * @return The long value of the specified string, otherwise -1 if there were any problems
     *     parsing the string as a long.
     */
    private static long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException nfe) {
            log.error("Unable to parse long value '" + s + "'", nfe);
        }

        return -1;
    }

    /** Perform a url decode on the given value if specified. Return value by default; */
    private static String decodeIfSpecified(String value, boolean decode) {
        return decode ? SdkHttpUtils.urlDecode(value) : value;
    }

    /**
     * Parses a ListBucket response XML document from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public ListBucketHandler parseListBucketObjectsResponse(
            InputStream inputStream, final boolean shouldSDKDecodeResponse) throws IOException {
        ListBucketHandler handler = new ListBucketHandler(shouldSDKDecodeResponse);
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));

        return handler;
    }

    /**
     * Parses a ListBucketV2 response XML document from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public ListObjectsV2Handler parseListObjectsV2Response(
            InputStream inputStream, final boolean shouldSDKDecodeResponse) throws IOException {
        ListObjectsV2Handler handler = new ListObjectsV2Handler(shouldSDKDecodeResponse);
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));

        return handler;
    }

    /**
     * Parses a ListVersions response XML document from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public ListVersionsHandler parseListVersionsResponse(
            InputStream inputStream, final boolean shouldSDKDecodeResponse) throws IOException {
        ListVersionsHandler handler = new ListVersionsHandler(shouldSDKDecodeResponse);
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }

    /**
     * Parses a ListAllMyBuckets response XML document from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public ListAllMyBucketsHandler parseListMyBucketsResponse(InputStream inputStream)
            throws IOException {
        ListAllMyBucketsHandler handler = new ListAllMyBucketsHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }

    /**
     * Parses an AccessControlListHandler response XML document from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public AccessControlListHandler parseAccessControlListResponse(InputStream inputStream)
            throws IOException {
        AccessControlListHandler handler = new AccessControlListHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    /**
     * Parses a LoggingStatus response XML document for a bucket from an input stream.
     *
     * @param inputStream XML data input stream.
     * @return the XML handler object populated with data parsed from the XML stream.
     * @throws SdkClientException
     */
    public BucketLoggingConfigurationHandler parseLoggingStatusResponse(InputStream inputStream)
            throws IOException {
        BucketLoggingConfigurationHandler handler = new BucketLoggingConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketLifecycleConfigurationHandler parseBucketLifecycleConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketLifecycleConfigurationHandler handler = new BucketLifecycleConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketCrossOriginConfigurationHandler parseBucketCrossOriginConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketCrossOriginConfigurationHandler handler = new BucketCrossOriginConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public String parseBucketLocationResponse(InputStream inputStream) throws IOException {
        BucketLocationHandler handler = new BucketLocationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler.getLocation();
    }

    public BucketVersioningConfigurationHandler parseVersioningConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketVersioningConfigurationHandler handler = new BucketVersioningConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketWebsiteConfigurationHandler parseWebsiteConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketWebsiteConfigurationHandler handler = new BucketWebsiteConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketReplicationConfigurationHandler parseReplicationConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketReplicationConfigurationHandler handler = new BucketReplicationConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketTaggingConfigurationHandler parseTaggingConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketTaggingConfigurationHandler handler = new BucketTaggingConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public BucketAccelerateConfigurationHandler parseAccelerateConfigurationResponse(
            InputStream inputStream) throws IOException {
        BucketAccelerateConfigurationHandler handler = new BucketAccelerateConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public DeleteObjectsHandler parseDeletedObjectsResult(InputStream inputStream)
            throws IOException {
        DeleteObjectsHandler handler = new DeleteObjectsHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public CopyObjectResultHandler parseCopyObjectResponse(InputStream inputStream)
            throws IOException {
        CopyObjectResultHandler handler = new CopyObjectResultHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public CompleteMultipartUploadHandler parseCompleteMultipartUploadResponse(
            InputStream inputStream) throws IOException {
        CompleteMultipartUploadHandler handler = new CompleteMultipartUploadHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public InitiateMultipartUploadHandler parseInitiateMultipartUploadResponse(
            InputStream inputStream) throws IOException {
        InitiateMultipartUploadHandler handler = new InitiateMultipartUploadHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ListMultipartUploadsHandler parseListMultipartUploadsResponse(InputStream inputStream)
            throws IOException {
        ListMultipartUploadsHandler handler = new ListMultipartUploadsHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ListPartsHandler parseListPartsResponse(InputStream inputStream) throws IOException {
        ListPartsHandler handler = new ListPartsHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public GetObjectTaggingHandler parseObjectTaggingResponse(InputStream inputStream)
            throws IOException {
        GetObjectTaggingHandler handler = new GetObjectTaggingHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public GetBucketMetricsConfigurationHandler parseGetBucketMetricsConfigurationResponse(
            InputStream inputStream) throws IOException {
        GetBucketMetricsConfigurationHandler handler = new GetBucketMetricsConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ListBucketMetricsConfigurationsHandler parseListBucketMetricsConfigurationsResponse(
            InputStream inputStream) throws IOException {
        ListBucketMetricsConfigurationsHandler handler =
                new ListBucketMetricsConfigurationsHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public GetBucketAnalyticsConfigurationHandler parseGetBucketAnalyticsConfigurationResponse(
            InputStream inputStream) throws IOException {
        GetBucketAnalyticsConfigurationHandler handler =
                new GetBucketAnalyticsConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ListBucketAnalyticsConfigurationHandler parseListBucketAnalyticsConfigurationResponse(
            InputStream inputStream) throws IOException {
        ListBucketAnalyticsConfigurationHandler handler =
                new ListBucketAnalyticsConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public GetBucketInventoryConfigurationHandler parseGetBucketInventoryConfigurationResponse(
            InputStream inputStream) throws IOException {
        GetBucketInventoryConfigurationHandler handler =
                new GetBucketInventoryConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ListBucketInventoryConfigurationsHandler parseBucketListInventoryConfigurationsResponse(
            InputStream inputStream) throws IOException {
        ListBucketInventoryConfigurationsHandler handler =
                new ListBucketInventoryConfigurationsHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    /**
     * @param inputStream
     * @return true if the bucket's is configured as Requester Pays, false if it is configured as
     *     Owner pays.
     * @throws SdkClientException
     */
    public RequestPaymentConfigurationHandler parseRequestPaymentConfigurationResponse(
            InputStream inputStream) throws IOException {
        RequestPaymentConfigurationHandler handler = new RequestPaymentConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    // ////////////
    // Handlers //
    // ////////////

    /** Handler for ListBucket response XML documents. */
    public static class ListBucketHandler extends AbstractHandler {
        private final ObjectListing objectListing = new ObjectListing();
        private final boolean shouldSDKDecodeResponse;

        private S3ObjectSummary currentObject = null;
        private Owner currentOwner = null;
        private String lastKey = null;

        public ListBucketHandler(final boolean shouldSDKDecodeResponse) {
            this.shouldSDKDecodeResponse = shouldSDKDecodeResponse;
        }

        public ObjectListing getObjectListing() {
            return objectListing;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListBucketResult")) {
                if (name.equals("Contents")) {
                    currentObject = new S3ObjectSummary();
                    currentObject.setBucketName(objectListing.getBucketName());
                }
            } else if (in("ListBucketResult", "Contents")) {
                if (name.equals("Owner")) {
                    currentOwner = new Owner();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (atTopLevel()) {
                if (name.equals("ListBucketResult")) {
                    /*
                     * S3 only includes the NextMarker XML element if the
                     * request specified a delimiter, but for consistency we'd
                     * like to always give easy access to the next marker if
                     * we're returning a list of results that's truncated.
                     */
                    if (objectListing.isTruncated() && objectListing.getNextMarker() == null) {

                        String nextMarker = null;
                        if (!objectListing.getObjectSummaries().isEmpty()) {
                            nextMarker =
                                    objectListing
                                            .getObjectSummaries()
                                            .get(objectListing.getObjectSummaries().size() - 1)
                                            .getKey();

                        } else if (!objectListing.getCommonPrefixes().isEmpty()) {
                            nextMarker =
                                    objectListing
                                            .getCommonPrefixes()
                                            .get(objectListing.getCommonPrefixes().size() - 1);
                        } else {
                            log.error(
                                    "S3 response indicates truncated results, "
                                            + "but contains no object summaries or "
                                            + "common prefixes.");
                        }

                        objectListing.setNextMarker(nextMarker);
                    }
                }
            } else if (in("ListBucketResult")) {
                if (name.equals("Name")) {
                    objectListing.setBucketName(getText());
                    if (log.isDebugEnabled()) {
                        log.debug("Examining listing for bucket: " + objectListing.getBucketName());
                    }

                } else if (name.equals("Prefix")) {
                    objectListing.setPrefix(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("Marker")) {
                    objectListing.setMarker(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("NextMarker")) {
                    objectListing.setNextMarker(
                            decodeIfSpecified(getText(), shouldSDKDecodeResponse));

                } else if (name.equals("MaxKeys")) {
                    objectListing.setMaxKeys(parseInt(getText()));

                } else if (name.equals("Delimiter")) {
                    objectListing.setDelimiter(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("EncodingType")) {
                    objectListing.setEncodingType(
                            shouldSDKDecodeResponse ? null : checkForEmptyString(getText()));
                } else if (name.equals("IsTruncated")) {
                    String isTruncatedStr = StringUtils.lowerCase(getText());

                    if (isTruncatedStr.startsWith("false")) {
                        objectListing.setTruncated(false);
                    } else if (isTruncatedStr.startsWith("true")) {
                        objectListing.setTruncated(true);
                    } else {
                        throw new IllegalStateException(
                                "Invalid value for IsTruncated field: " + isTruncatedStr);
                    }

                } else if (name.equals("Contents")) {
                    objectListing.getObjectSummaries().add(currentObject);
                    currentObject = null;
                }
            } else if (in("ListBucketResult", "Contents")) {
                if (name.equals("Key")) {
                    lastKey = getText();
                    currentObject.setKey(decodeIfSpecified(lastKey, shouldSDKDecodeResponse));
                } else if (name.equals("LastModified")) {
                    currentObject.setLastModified(ServiceUtils.parseIso8601Date(getText()));

                } else if (name.equals("ETag")) {
                    currentObject.setETag(ServiceUtils.removeQuotes(getText()));

                } else if (name.equals("Size")) {
                    currentObject.setSize(parseLong(getText()));

                } else if (name.equals("StorageClass")) {
                    currentObject.setStorageClass(getText());

                } else if (name.equals("Owner")) {
                    currentObject.setOwner(currentOwner);
                    currentOwner = null;
                }
            } else if (in("ListBucketResult", "Contents", "Owner")) {
                if (name.equals("ID")) {
                    currentOwner.setId(getText());

                } else if (name.equals("DisplayName")) {
                    currentOwner.setDisplayName(getText());
                }
            } else if (in("ListBucketResult", "CommonPrefixes")) {
                if (name.equals("Prefix")) {
                    objectListing
                            .getCommonPrefixes()
                            .add(decodeIfSpecified(getText(), shouldSDKDecodeResponse));
                }
            }
        }
    }

    /** Handler for ListObjectsV2 response XML documents. */
    public static class ListObjectsV2Handler extends AbstractHandler {
        private final ListObjectsV2Result result = new ListObjectsV2Result();
        private final boolean shouldSDKDecodeResponse;

        private S3ObjectSummary currentObject = null;
        private Owner currentOwner = null;
        private String lastKey = null;

        public ListObjectsV2Handler(final boolean shouldSDKDecodeResponse) {
            this.shouldSDKDecodeResponse = shouldSDKDecodeResponse;
        }

        public ListObjectsV2Result getResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListBucketResult")) {
                if (name.equals("Contents")) {
                    currentObject = new S3ObjectSummary();
                    currentObject.setBucketName(result.getBucketName());
                }
            } else if (in("ListBucketResult", "Contents")) {
                if (name.equals("Owner")) {
                    currentOwner = new Owner();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (atTopLevel()) {
                if (name.equals("ListBucketResult")) {
                    /*
                     * S3 only includes the NextContinuationToken XML element if the
                     * request specified a delimiter, but for consistency we'd
                     * like to always give easy access to the next token if
                     * we're returning a list of results that's truncated.
                     */
                    if (result.isTruncated() && result.getNextContinuationToken() == null) {

                        String nextContinuationToken = null;
                        if (!result.getObjectSummaries().isEmpty()) {
                            nextContinuationToken =
                                    result.getObjectSummaries()
                                            .get(result.getObjectSummaries().size() - 1)
                                            .getKey();

                        } else {
                            log.error(
                                    "S3 response indicates truncated results, "
                                            + "but contains no object summaries.");
                        }

                        result.setNextContinuationToken(nextContinuationToken);
                    }
                }
            } else if (in("ListBucketResult")) {
                if (name.equals("Name")) {
                    result.setBucketName(getText());
                    if (log.isDebugEnabled()) {
                        log.debug("Examining listing for bucket: " + result.getBucketName());
                    }

                } else if (name.equals("Prefix")) {
                    result.setPrefix(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("MaxKeys")) {
                    result.setMaxKeys(parseInt(getText()));

                } else if (name.equals("NextContinuationToken")) {
                    result.setNextContinuationToken(getText());

                } else if (name.equals("ContinuationToken")) {
                    result.setContinuationToken(getText());

                } else if (name.equals("StartAfter")) {
                    result.setStartAfter(decodeIfSpecified(getText(), shouldSDKDecodeResponse));

                } else if (name.equals("KeyCount")) {
                    result.setKeyCount(parseInt(getText()));

                } else if (name.equals("Delimiter")) {
                    result.setDelimiter(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("EncodingType")) {
                    result.setEncodingType(checkForEmptyString(getText()));
                } else if (name.equals("IsTruncated")) {
                    String isTruncatedStr = StringUtils.lowerCase(getText());

                    if (isTruncatedStr.startsWith("false")) {
                        result.setTruncated(false);
                    } else if (isTruncatedStr.startsWith("true")) {
                        result.setTruncated(true);
                    } else {
                        throw new IllegalStateException(
                                "Invalid value for IsTruncated field: " + isTruncatedStr);
                    }

                } else if (name.equals("Contents")) {
                    result.getObjectSummaries().add(currentObject);
                    currentObject = null;
                }
            } else if (in("ListBucketResult", "Contents")) {
                if (name.equals("Key")) {
                    lastKey = getText();
                    currentObject.setKey(decodeIfSpecified(lastKey, shouldSDKDecodeResponse));
                } else if (name.equals("LastModified")) {
                    currentObject.setLastModified(ServiceUtils.parseIso8601Date(getText()));

                } else if (name.equals("ETag")) {
                    currentObject.setETag(ServiceUtils.removeQuotes(getText()));

                } else if (name.equals("Size")) {
                    currentObject.setSize(parseLong(getText()));

                } else if (name.equals("StorageClass")) {
                    currentObject.setStorageClass(getText());

                } else if (name.equals("Owner")) {
                    currentObject.setOwner(currentOwner);
                    currentOwner = null;
                }
            } else if (in("ListBucketResult", "Contents", "Owner")) {
                if (name.equals("ID")) {
                    currentOwner.setId(getText());

                } else if (name.equals("DisplayName")) {
                    currentOwner.setDisplayName(getText());
                }
            } else if (in("ListBucketResult", "CommonPrefixes")) {
                if (name.equals("Prefix")) {
                    result.getCommonPrefixes()
                            .add(decodeIfSpecified(getText(), shouldSDKDecodeResponse));
                }
            }
        }
    }

    /**
     * Handler for ListAllMyBuckets response XML documents. The document is parsed into {@link
     * Bucket}s available via the {@link #getBuckets()} method.
     */
    public static class ListAllMyBucketsHandler extends AbstractHandler {

        private final List<Bucket> buckets = new ArrayList<Bucket>();
        private Owner bucketsOwner = null;

        private Bucket currentBucket = null;

        /** @return the buckets listed in the document. */
        public List<Bucket> getBuckets() {
            return buckets;
        }

        /** @return the owner of the buckets. */
        public Owner getOwner() {
            return bucketsOwner;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListAllMyBucketsResult")) {
                if (name.equals("Owner")) {
                    bucketsOwner = new Owner();
                }
            } else if (in("ListAllMyBucketsResult", "Buckets")) {
                if (name.equals("Bucket")) {
                    currentBucket = new Bucket();
                    currentBucket.setOwner(bucketsOwner);
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("ListAllMyBucketsResult", "Owner")) {
                if (name.equals("ID")) {
                    bucketsOwner.setId(getText());

                } else if (name.equals("DisplayName")) {
                    bucketsOwner.setDisplayName(getText());
                }
            } else if (in("ListAllMyBucketsResult", "Buckets")) {
                if (name.equals("Bucket")) {
                    buckets.add(currentBucket);
                    currentBucket = null;
                }
            } else if (in("ListAllMyBucketsResult", "Buckets", "Bucket")) {
                if (name.equals("Name")) {
                    currentBucket.setName(getText());

                } else if (name.equals("CreationDate")) {
                    Date creationDate = DateUtils.parseISO8601Date(getText());
                    currentBucket.setCreationDate(creationDate);
                }
            }
        }
    }

    /**
     * Handler for AccessControlList response XML documents. The document is parsed into an {@link
     * AccessControlList} object available via the {@link #getAccessControlList()} method.
     */
    public static class AccessControlListHandler extends AbstractHandler {

        private final AccessControlList accessControlList = new AccessControlList();

        private Grantee currentGrantee = null;
        private Permission currentPermission = null;

        /** @return an object representing the ACL document. */
        public AccessControlList getAccessControlList() {
            return accessControlList;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("AccessControlPolicy")) {
                if (name.equals("Owner")) {
                    accessControlList.setOwner(new Owner());
                }
            } else if (in("AccessControlPolicy", "AccessControlList", "Grant")) {
                if (name.equals("Grantee")) {
                    String type = XmlResponsesSaxParser.findAttributeValue("xsi:type", attrs);

                    if ("AmazonCustomerByEmail".equals(type)) {
                        currentGrantee = new EmailAddressGrantee(null);
                    } else if ("CanonicalUser".equals(type)) {
                        currentGrantee = new CanonicalGrantee(null);
                    } else if ("Group".equals(type)) {
                        /*
                         * Nothing to do for GroupGrantees here since we
                         * can't construct an empty enum value early.
                         */
                    }
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("AccessControlPolicy", "Owner")) {
                if (name.equals("ID")) {
                    accessControlList.getOwner().setId(getText());
                } else if (name.equals("DisplayName")) {
                    accessControlList.getOwner().setDisplayName(getText());
                }
            } else if (in("AccessControlPolicy", "AccessControlList")) {
                if (name.equals("Grant")) {
                    accessControlList.grantPermission(currentGrantee, currentPermission);

                    currentGrantee = null;
                    currentPermission = null;
                }
            } else if (in("AccessControlPolicy", "AccessControlList", "Grant")) {
                if (name.equals("Permission")) {
                    currentPermission = Permission.parsePermission(getText());
                }
            } else if (in("AccessControlPolicy", "AccessControlList", "Grant", "Grantee")) {
                if (name.equals("ID")) {
                    currentGrantee.setIdentifier(getText());

                } else if (name.equals("EmailAddress")) {
                    currentGrantee.setIdentifier(getText());

                } else if (name.equals("URI")) {
                    /*
                     * Only GroupGrantees contain an URI element in them, and we
                     * can't construct currentGrantee during startElement for a
                     * GroupGrantee since it's an enum.
                     */
                    currentGrantee = GroupGrantee.parseGroupGrantee(getText());

                } else if (name.equals("DisplayName")) {
                    ((CanonicalGrantee) currentGrantee).setDisplayName(getText());
                }
            }
        }
    }

    /**
     * Handler for LoggingStatus response XML documents for a bucket. The document is parsed into an
     * {@link BucketLoggingConfiguration} object available via the {@link
     * #getBucketLoggingConfiguration()} method.
     */
    public static class BucketLoggingConfigurationHandler extends AbstractHandler {

        private final BucketLoggingConfiguration bucketLoggingConfiguration =
                new BucketLoggingConfiguration();

        /** @return an object representing the bucket's LoggingStatus document. */
        public BucketLoggingConfiguration getBucketLoggingConfiguration() {
            return bucketLoggingConfiguration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("BucketLoggingStatus", "LoggingEnabled")) {
                if (name.equals("TargetBucket")) {
                    bucketLoggingConfiguration.setDestinationBucketName(getText());

                } else if (name.equals("TargetPrefix")) {
                    bucketLoggingConfiguration.setLogFilePrefix(getText());
                }
            }
        }
    }

    /**
     * Handler for CreateBucketConfiguration response XML documents for a bucket. The document is
     * parsed into a String representing the bucket's location, available via the {@link
     * #getLocation()} method.
     */
    public static class BucketLocationHandler extends AbstractHandler {

        private String location = null;

        /** @return the bucket's location. */
        public String getLocation() {
            return location;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (atTopLevel()) {
                if (name.equals("LocationConstraint")) {
                    String elementText = getText();
                    if (elementText.length() == 0) {
                        location = null;
                    } else {
                        location = elementText;
                    }
                }
            }
        }
    }

    public static class CopyObjectResultHandler extends AbstractSSEHandler
            implements ObjectExpirationResult, S3RequesterChargedResult, S3VersionResult {

        // Data items for successful copy
        private final CopyObjectResult result = new CopyObjectResult();

        // Data items for failed copy
        private String errorCode = null;
        private String errorMessage = null;
        private String errorRequestId = null;
        private String errorHostId = null;
        private boolean receivedErrorResponse = false;

        @Override
        protected ServerSideEncryptionResult sseResult() {
            return result;
        }

        public Date getLastModified() {
            return result.getLastModifiedDate();
        }

        @Override
        public String getVersionId() {
            return result.getVersionId();
        }

        @Override
        public void setVersionId(String versionId) {
            result.setVersionId(versionId);
        }

        @Override
        public Date getExpirationTime() {
            return result.getExpirationTime();
        }

        @Override
        public void setExpirationTime(Date expirationTime) {
            result.setExpirationTime(expirationTime);
        }

        @Override
        public String getExpirationTimeRuleId() {
            return result.getExpirationTimeRuleId();
        }

        @Override
        public void setExpirationTimeRuleId(String expirationTimeRuleId) {
            result.setExpirationTimeRuleId(expirationTimeRuleId);
        }

        public String getETag() {
            return result.getETag();
        }

        public String getErrorCode() {
            return errorCode;
        }

        public String getErrorHostId() {
            return errorHostId;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getErrorRequestId() {
            return errorRequestId;
        }

        public boolean isErrorResponse() {
            return receivedErrorResponse;
        }

        public boolean isRequesterCharged() {
            return result.isRequesterCharged();
        }

        public void setRequesterCharged(boolean isRequesterCharged) {
            result.setRequesterCharged(isRequesterCharged);
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (atTopLevel()) {
                if (name.equals("CopyObjectResult") || name.equals("CopyPartResult")) {
                    receivedErrorResponse = false;
                } else if (name.equals("Error")) {
                    receivedErrorResponse = true;
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("CopyObjectResult") || in("CopyPartResult")) {
                if (name.equals("LastModified")) {
                    result.setLastModifiedDate(ServiceUtils.parseIso8601Date(getText()));
                } else if (name.equals("ETag")) {
                    result.setETag(ServiceUtils.removeQuotes(getText()));
                }
            } else if (in("Error")) {
                if (name.equals("Code")) {
                    errorCode = getText();
                } else if (name.equals("Message")) {
                    errorMessage = getText();
                } else if (name.equals("RequestId")) {
                    errorRequestId = getText();
                } else if (name.equals("HostId")) {
                    errorHostId = getText();
                }
            }
        }
    }

    /**
     * Handler for parsing RequestPaymentConfiguration XML response associated with an Amazon S3
     * bucket. The XML response is parsed into a <code>RequestPaymentConfiguration</code> object.
     */
    public static class RequestPaymentConfigurationHandler extends AbstractHandler {

        private String payer = null;

        public RequestPaymentConfiguration getConfiguration() {
            return new RequestPaymentConfiguration(Payer.valueOf(payer));
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("RequestPaymentConfiguration")) {
                if (name.equals("Payer")) {
                    payer = getText();
                }
            }
        }
    }

    /** Handler for ListVersionsResult XML document. */
    public static class ListVersionsHandler extends AbstractHandler {

        private final VersionListing versionListing = new VersionListing();
        private final boolean shouldSDKDecodeResponse;

        private S3VersionSummary currentVersionSummary;
        private Owner currentOwner;

        public ListVersionsHandler(final boolean shouldSDKDecodeResponse) {
            this.shouldSDKDecodeResponse = shouldSDKDecodeResponse;
        }

        public VersionListing getListing() {
            return versionListing;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListVersionsResult")) {
                if (name.equals("Version")) {
                    currentVersionSummary = new S3VersionSummary();
                    currentVersionSummary.setBucketName(versionListing.getBucketName());

                } else if (name.equals("DeleteMarker")) {
                    currentVersionSummary = new S3VersionSummary();
                    currentVersionSummary.setBucketName(versionListing.getBucketName());
                    currentVersionSummary.setIsDeleteMarker(true);
                }
            } else if (in("ListVersionsResult", "Version")
                    || in("ListVersionsResult", "DeleteMarker")) {
                if (name.equals("Owner")) {
                    currentOwner = new Owner();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {

            if (in("ListVersionsResult")) {
                if (name.equals("Name")) {
                    versionListing.setBucketName(getText());

                } else if (name.equals("Prefix")) {
                    versionListing.setPrefix(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));
                } else if (name.equals("KeyMarker")) {
                    versionListing.setKeyMarker(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));
                } else if (name.equals("VersionIdMarker")) {
                    versionListing.setVersionIdMarker(checkForEmptyString(getText()));

                } else if (name.equals("MaxKeys")) {
                    versionListing.setMaxKeys(Integer.parseInt(getText()));

                } else if (name.equals("Delimiter")) {
                    versionListing.setDelimiter(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("EncodingType")) {
                    versionListing.setEncodingType(
                            shouldSDKDecodeResponse ? null : checkForEmptyString(getText()));
                } else if (name.equals("NextKeyMarker")) {
                    versionListing.setNextKeyMarker(
                            decodeIfSpecified(
                                    checkForEmptyString(getText()), shouldSDKDecodeResponse));

                } else if (name.equals("NextVersionIdMarker")) {
                    versionListing.setNextVersionIdMarker(getText());

                } else if (name.equals("IsTruncated")) {
                    versionListing.setTruncated("true".equals(getText()));

                } else if (name.equals("Version") || name.equals("DeleteMarker")) {

                    versionListing.getVersionSummaries().add(currentVersionSummary);

                    currentVersionSummary = null;
                }
            } else if (in("ListVersionsResult", "CommonPrefixes")) {
                if (name.equals("Prefix")) {
                    final String commonPrefix = checkForEmptyString(getText());
                    versionListing
                            .getCommonPrefixes()
                            .add(
                                    shouldSDKDecodeResponse
                                            ? SdkHttpUtils.urlDecode(commonPrefix)
                                            : commonPrefix);
                }
            } else if (in("ListVersionsResult", "Version")
                    || in("ListVersionsResult", "DeleteMarker")) {

                if (name.equals("Key")) {
                    currentVersionSummary.setKey(
                            decodeIfSpecified(getText(), shouldSDKDecodeResponse));

                } else if (name.equals("VersionId")) {
                    currentVersionSummary.setVersionId(getText());

                } else if (name.equals("IsLatest")) {
                    currentVersionSummary.setIsLatest("true".equals(getText()));

                } else if (name.equals("LastModified")) {
                    currentVersionSummary.setLastModified(ServiceUtils.parseIso8601Date(getText()));

                } else if (name.equals("ETag")) {
                    currentVersionSummary.setETag(ServiceUtils.removeQuotes(getText()));

                } else if (name.equals("Size")) {
                    currentVersionSummary.setSize(Long.parseLong(getText()));

                } else if (name.equals("Owner")) {
                    currentVersionSummary.setOwner(currentOwner);
                    currentOwner = null;

                } else if (name.equals("StorageClass")) {
                    currentVersionSummary.setStorageClass(getText());
                }
            } else if (in("ListVersionsResult", "Version", "Owner")
                    || in("ListVersionsResult", "DeleteMarker", "Owner")) {

                if (name.equals("ID")) {
                    currentOwner.setId(getText());
                } else if (name.equals("DisplayName")) {
                    currentOwner.setDisplayName(getText());
                }
            }
        }
    }

    public static class BucketWebsiteConfigurationHandler extends AbstractHandler {

        private final BucketWebsiteConfiguration configuration =
                new BucketWebsiteConfiguration(null);

        private RoutingRuleCondition currentCondition = null;
        private RedirectRule currentRedirectRule = null;
        private RoutingRule currentRoutingRule = null;

        public BucketWebsiteConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("WebsiteConfiguration")) {
                if (name.equals("RedirectAllRequestsTo")) {
                    currentRedirectRule = new RedirectRule();
                }
            } else if (in("WebsiteConfiguration", "RoutingRules")) {
                if (name.equals("RoutingRule")) {
                    currentRoutingRule = new RoutingRule();
                }
            } else if (in("WebsiteConfiguration", "RoutingRules", "RoutingRule")) {
                if (name.equals("Condition")) {
                    currentCondition = new RoutingRuleCondition();
                } else if (name.equals("Redirect")) {
                    currentRedirectRule = new RedirectRule();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("WebsiteConfiguration")) {
                if (name.equals("RedirectAllRequestsTo")) {
                    configuration.setRedirectAllRequestsTo(currentRedirectRule);
                    currentRedirectRule = null;
                }
            } else if (in("WebsiteConfiguration", "IndexDocument")) {
                if (name.equals("Suffix")) {
                    configuration.setIndexDocumentSuffix(getText());
                }
            } else if (in("WebsiteConfiguration", "ErrorDocument")) {
                if (name.equals("Key")) {
                    configuration.setErrorDocument(getText());
                }
            } else if (in("WebsiteConfiguration", "RoutingRules")) {
                if (name.equals("RoutingRule")) {
                    configuration.getRoutingRules().add(currentRoutingRule);
                    currentRoutingRule = null;
                }
            } else if (in("WebsiteConfiguration", "RoutingRules", "RoutingRule")) {
                if (name.equals("Condition")) {
                    currentRoutingRule.setCondition(currentCondition);
                    currentCondition = null;
                } else if (name.equals("Redirect")) {
                    currentRoutingRule.setRedirect(currentRedirectRule);
                    currentRedirectRule = null;
                }
            } else if (in("WebsiteConfiguration", "RoutingRules", "RoutingRule", "Condition")) {
                if (name.equals("KeyPrefixEquals")) {
                    currentCondition.setKeyPrefixEquals(getText());
                } else if (name.equals("HttpErrorCodeReturnedEquals")) {
                    currentCondition.setHttpErrorCodeReturnedEquals(getText());
                }
            } else if (in("WebsiteConfiguration", "RedirectAllRequestsTo")
                    || in("WebsiteConfiguration", "RoutingRules", "RoutingRule", "Redirect")) {

                if (name.equals("Protocol")) {
                    currentRedirectRule.setProtocol(getText());

                } else if (name.equals("HostName")) {
                    currentRedirectRule.setHostName(getText());

                } else if (name.equals("ReplaceKeyPrefixWith")) {
                    currentRedirectRule.setReplaceKeyPrefixWith(getText());

                } else if (name.equals("ReplaceKeyWith")) {
                    currentRedirectRule.setReplaceKeyWith(getText());

                } else if (name.equals("HttpRedirectCode")) {
                    currentRedirectRule.setHttpRedirectCode(getText());
                }
            }
        }
    }

    public static class BucketVersioningConfigurationHandler extends AbstractHandler {

        private final BucketVersioningConfiguration configuration =
                new BucketVersioningConfiguration();

        public BucketVersioningConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("VersioningConfiguration")) {
                if (name.equals("Status")) {
                    configuration.setStatus(getText());

                } else if (name.equals("MfaDelete")) {
                    String mfaDeleteStatus = getText();

                    if (mfaDeleteStatus.equals("Disabled")) {
                        configuration.setMfaDeleteEnabled(false);
                    } else if (mfaDeleteStatus.equals("Enabled")) {
                        configuration.setMfaDeleteEnabled(true);
                    } else {
                        configuration.setMfaDeleteEnabled(null);
                    }
                }
            }
        }
    }

    public static class BucketAccelerateConfigurationHandler extends AbstractHandler {

        private final BucketAccelerateConfiguration configuration =
                new BucketAccelerateConfiguration((String) null);

        public BucketAccelerateConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("AccelerateConfiguration")) {
                if (name.equals("Status")) {
                    configuration.setStatus(getText());
                }
            }
        }
    }

    /*
     * <?xml version="1.0" encoding="UTF-8"?>
     * <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     *     <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
     *     <Bucket>Example-Bucket</Bucket>
     *     <Key>Example-Object</Key>
     *     <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
     * </CompleteMultipartUploadResult>
     *
     * Or if an error occurred while completing:
     *
     * <?xml version="1.0" encoding="UTF-8"?>
     * <Error>
     *     <Code>InternalError</Code>
     *     <Message>We encountered an internal error. Please try again.</Message>
     *     <RequestId>656c76696e6727732072657175657374</RequestId>
     *     <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
     * </Error>
     */
    public static class CompleteMultipartUploadHandler extends AbstractSSEHandler
            implements ObjectExpirationResult, S3VersionResult, S3RequesterChargedResult {
        // Successful completion
        private CompleteMultipartUploadResult result;

        // Error during completion
        private AmazonS3Exception ase;
        private String hostId;
        private String requestId;
        private String errorCode;

        @Override
        protected ServerSideEncryptionResult sseResult() {
            return result;
        }
        /**
         * @see com.amazonaws.services.s3.model.CompleteMultipartUploadResult#getExpirationTime()
         */
        @Override
        public Date getExpirationTime() {
            return result == null ? null : result.getExpirationTime();
        }

        /**
         * @see
         *     com.amazonaws.services.s3.model.CompleteMultipartUploadResult#setExpirationTime(java.util.Date)
         */
        @Override
        public void setExpirationTime(Date expirationTime) {
            if (result != null) {
                result.setExpirationTime(expirationTime);
            }
        }

        /**
         * @see
         *     com.amazonaws.services.s3.model.CompleteMultipartUploadResult#getExpirationTimeRuleId()
         */
        @Override
        public String getExpirationTimeRuleId() {
            return result == null ? null : result.getExpirationTimeRuleId();
        }

        /**
         * @see
         *     com.amazonaws.services.s3.model.CompleteMultipartUploadResult#setExpirationTimeRuleId(java.lang.String)
         */
        @Override
        public void setExpirationTimeRuleId(String expirationTimeRuleId) {
            if (result != null) {
                result.setExpirationTimeRuleId(expirationTimeRuleId);
            }
        }

        @Override
        public void setVersionId(String versionId) {
            if (result != null) {
                result.setVersionId(versionId);
            }
        }

        @Override
        public String getVersionId() {
            return result == null ? null : result.getVersionId();
        }

        /**
         * @see com.amazonaws.services.s3.model.CompleteMultipartUploadResult#isRequesterCharged()
         */
        public boolean isRequesterCharged() {
            return result == null ? false : result.isRequesterCharged();
        }

        /**
         * @see
         *     com.amazonaws.services.s3.model.CompleteMultipartUploadResult#setRequesterCharged(boolean)
         */
        public void setRequesterCharged(boolean isRequesterCharged) {
            if (result != null) {
                result.setRequesterCharged(isRequesterCharged);
            }
        }

        public CompleteMultipartUploadResult getCompleteMultipartUploadResult() {
            return result;
        }

        public AmazonS3Exception getAmazonS3Exception() {
            return ase;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (atTopLevel()) {
                if (name.equals("CompleteMultipartUploadResult")) {
                    result = new CompleteMultipartUploadResult();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (atTopLevel()) {
                if (name.equals("Error")) {
                    if (ase != null) {
                        ase.setErrorCode(errorCode);
                        ase.setRequestId(requestId);
                        ase.setExtendedRequestId(hostId);
                    }
                }
            } else if (in("CompleteMultipartUploadResult")) {
                if (name.equals("Location")) {
                    result.setLocation(getText());
                } else if (name.equals("Bucket")) {
                    result.setBucketName(getText());
                } else if (name.equals("Key")) {
                    result.setKey(getText());
                } else if (name.equals("ETag")) {
                    result.setETag(ServiceUtils.removeQuotes(getText()));
                }
            } else if (in("Error")) {
                if (name.equals("Code")) {
                    errorCode = getText();
                } else if (name.equals("Message")) {
                    ase = new AmazonS3Exception(getText());
                } else if (name.equals("RequestId")) {
                    requestId = getText();
                } else if (name.equals("HostId")) {
                    hostId = getText();
                }
            }
        }
    }

    /*
     * <?xml version="1.0" encoding="UTF-8"?>
     * <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     *     <Bucket>example-bucket</Bucket>
     *     <Key>example-object</Key>
     *     <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
     * </InitiateMultipartUploadResult>
     */
    public static class InitiateMultipartUploadHandler extends AbstractHandler {

        private final InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();

        public InitiateMultipartUploadResult getInitiateMultipartUploadResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {}

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("InitiateMultipartUploadResult")) {
                if (name.equals("Bucket")) {
                    result.setBucketName(getText());

                } else if (name.equals("Key")) {
                    result.setKey(getText());

                } else if (name.equals("UploadId")) {
                    result.setUploadId(getText());
                }
            }
        }
    }

    /*
     * HTTP/1.1 200 OK
     * x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
     * x-amz-request-id: 656c76696e6727732072657175657374
     * Date: Tue, 16 Feb 2010 20:34:56 GMT
     * Content-Length: 1330
     * Connection: keep-alive
     * Server: AmazonS3
     *
     * <?xml version="1.0" encoding="UTF-8"?>
     * <ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     *     <Bucket>bucket</Bucket>
     *     <KeyMarker></KeyMarker>
     *     <Delimiter>/</Delimiter>
     *     <Prefix/>
     *     <UploadIdMarker></UploadIdMarker>
     *     <NextKeyMarker>my-movie.m2ts</NextKeyMarker>
     *     <NextUploadIdMarker>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</NextUploadIdMarker>
     *     <MaxUploads>3</MaxUploads>
     *     <IsTruncated>true</IsTruncated>
     *     <Upload>
     *         <Key>my-divisor</Key>
     *         <UploadId>XMgbGlrZSBlbHZpbmcncyBub3QgaGF2aW5nIG11Y2ggbHVjaw</UploadId>
     *         <Owner>
     *             <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
     *             <DisplayName>delving</DisplayName>
     *         </Owner>
     *         <StorageClass>STANDARD</StorageClass>
     *         <Initiated>Tue, 26 Jan 2010 19:42:19 GMT</Initiated>
     *     </Upload>
     *     <Upload>
     *         <Key>my-movie.m2ts</Key>
     *         <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
     *         <Owner>
     *             <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
     *             <DisplayName>delving</DisplayName>
     *         </Owner>
     *         <StorageClass>STANDARD</StorageClass>
     *         <Initiated>Tue, 16 Feb 2010 20:34:56 GMT</Initiated>
     *     </Upload>
     *     <Upload>
     *         <Key>my-movie.m2ts</Key>
     *         <UploadId>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</UploadId>
     *         <Owner>
     *             <ID>b1d16700c70b0b05597d7acd6a3f92be</ID>
     *             <DisplayName>delving</DisplayName>
     *         </Owner>
     *         <StorageClass>STANDARD</StorageClass>
     *         <Initiated>Wed, 27 Jan 2010 03:02:01 GMT</Initiated>
     *     </Upload>
     *    <CommonPrefixes>
     *        <Prefix>photos/</Prefix>
     *    </CommonPrefixes>
     *    <CommonPrefixes>
     *        <Prefix>videos/</Prefix>
     *    </CommonPrefixes>
     * </ListMultipartUploadsResult>
     */
    public static class ListMultipartUploadsHandler extends AbstractHandler {

        private final MultipartUploadListing result = new MultipartUploadListing();

        private MultipartUpload currentMultipartUpload;
        private Owner currentOwner;

        public MultipartUploadListing getListMultipartUploadsResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListMultipartUploadsResult")) {
                if (name.equals("Upload")) {
                    currentMultipartUpload = new MultipartUpload();
                }
            } else if (in("ListMultipartUploadsResult", "Upload")) {
                if (name.equals("Owner") || name.equals("Initiator")) {
                    currentOwner = new Owner();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("ListMultipartUploadsResult")) {
                if (name.equals("Bucket")) {
                    result.setBucketName(getText());
                } else if (name.equals("KeyMarker")) {
                    result.setKeyMarker(checkForEmptyString(getText()));
                } else if (name.equals("Delimiter")) {
                    result.setDelimiter(checkForEmptyString(getText()));
                } else if (name.equals("Prefix")) {
                    result.setPrefix(checkForEmptyString(getText()));
                } else if (name.equals("UploadIdMarker")) {
                    result.setUploadIdMarker(checkForEmptyString(getText()));
                } else if (name.equals("NextKeyMarker")) {
                    result.setNextKeyMarker(checkForEmptyString(getText()));
                } else if (name.equals("NextUploadIdMarker")) {
                    result.setNextUploadIdMarker(checkForEmptyString(getText()));
                } else if (name.equals("MaxUploads")) {
                    result.setMaxUploads(Integer.parseInt(getText()));
                } else if (name.equals("EncodingType")) {
                    result.setEncodingType(checkForEmptyString(getText()));
                } else if (name.equals("IsTruncated")) {
                    result.setTruncated(Boolean.parseBoolean(getText()));
                } else if (name.equals("Upload")) {
                    result.getMultipartUploads().add(currentMultipartUpload);
                    currentMultipartUpload = null;
                }
            } else if (in("ListMultipartUploadsResult", "CommonPrefixes")) {
                if (name.equals("Prefix")) {
                    result.getCommonPrefixes().add(getText());
                }
            } else if (in("ListMultipartUploadsResult", "Upload")) {
                if (name.equals("Key")) {
                    currentMultipartUpload.setKey(getText());
                } else if (name.equals("UploadId")) {
                    currentMultipartUpload.setUploadId(getText());
                } else if (name.equals("Owner")) {
                    currentMultipartUpload.setOwner(currentOwner);
                    currentOwner = null;
                } else if (name.equals("Initiator")) {
                    currentMultipartUpload.setInitiator(currentOwner);
                    currentOwner = null;
                } else if (name.equals("StorageClass")) {
                    currentMultipartUpload.setStorageClass(getText());
                } else if (name.equals("Initiated")) {
                    currentMultipartUpload.setInitiated(ServiceUtils.parseIso8601Date(getText()));
                }
            } else if (in("ListMultipartUploadsResult", "Upload", "Owner")
                    || in("ListMultipartUploadsResult", "Upload", "Initiator")) {

                if (name.equals("ID")) {
                    currentOwner.setId(checkForEmptyString(getText()));
                } else if (name.equals("DisplayName")) {
                    currentOwner.setDisplayName(checkForEmptyString(getText()));
                }
            }
        }
    }

    /*
     * HTTP/1.1 200 OK
     * x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
     * x-amz-request-id: 656c76696e6727732072657175657374
     * Date: Tue, 16 Feb 2010 20:34:56 GMT
     * Content-Length: 985
     * Connection: keep-alive
     * Server: AmazonS3
     *
     * <?xml version="1.0" encoding="UTF-8"?>
     * <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     *     <Bucket>example-bucket</Bucket>
     *     <Key>example-object</Key>
     *     <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
     *     <Owner>
     *         <ID>x1x16700c70b0b05597d7ecd6a3f92be</ID>
     *         <DisplayName>username</DisplayName>
     *     </Owner>
     *     <Initiator>
     *         <ID>x1x16700c70b0b05597d7ecd6a3f92be</ID>
     *         <DisplayName>username</DisplayName>
     *     </Initiator>
     *     <StorageClass>STANDARD</StorageClass>
     *     <PartNumberMarker>1</PartNumberMarker>
     *     <NextPartNumberMarker>3</NextPartNumberMarker>
     *     <MaxParts>2</MaxParts>
     *     <IsTruncated>true</IsTruncated>
     *     <Part>
     *         <PartNumber>2</PartNumber>
     *         <LastModified>Wed, 27 Jan 2010 03:02:03 GMT</LastModified>
     *         <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
     *         <Size>10485760</Size>
     *     </Part>
     *     <Part>
     *        <PartNumber>3</PartNumber>
     *        <LastModified>Wed, 27 Jan 2010 03:02:02 GMT</LastModified>
     *        <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
     *        <Size>10485760</Size>
     *     </Part>
     * </ListPartsResult>
     */
    public static class ListPartsHandler extends AbstractHandler {

        private final PartListing result = new PartListing();

        private PartSummary currentPart;
        private Owner currentOwner;

        public PartListing getListPartsResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListPartsResult")) {
                if (name.equals("Part")) {
                    currentPart = new PartSummary();
                } else if (name.equals("Owner") || name.equals("Initiator")) {
                    currentOwner = new Owner();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("ListPartsResult")) {
                if (name.equals("Bucket")) {
                    result.setBucketName(getText());
                } else if (name.equals("Key")) {
                    result.setKey(getText());
                } else if (name.equals("UploadId")) {
                    result.setUploadId(getText());
                } else if (name.equals("Owner")) {
                    result.setOwner(currentOwner);
                    currentOwner = null;
                } else if (name.equals("Initiator")) {
                    result.setInitiator(currentOwner);
                    currentOwner = null;
                } else if (name.equals("StorageClass")) {
                    result.setStorageClass(getText());
                } else if (name.equals("PartNumberMarker")) {
                    result.setPartNumberMarker(parseInteger(getText()));
                } else if (name.equals("NextPartNumberMarker")) {
                    result.setNextPartNumberMarker(parseInteger(getText()));
                } else if (name.equals("MaxParts")) {
                    result.setMaxParts(parseInteger(getText()));
                } else if (name.equals("EncodingType")) {
                    result.setEncodingType(checkForEmptyString(getText()));
                } else if (name.equals("IsTruncated")) {
                    result.setTruncated(Boolean.parseBoolean(getText()));
                } else if (name.equals("Part")) {
                    result.getParts().add(currentPart);
                    currentPart = null;
                }
            } else if (in("ListPartsResult", "Part")) {
                if (name.equals("PartNumber")) {
                    currentPart.setPartNumber(Integer.parseInt(getText()));
                } else if (name.equals("LastModified")) {
                    currentPart.setLastModified(ServiceUtils.parseIso8601Date(getText()));
                } else if (name.equals("ETag")) {
                    currentPart.setETag(ServiceUtils.removeQuotes(getText()));
                } else if (name.equals("Size")) {
                    currentPart.setSize(Long.parseLong(getText()));
                }
            } else if (in("ListPartsResult", "Owner") || in("ListPartsResult", "Initiator")) {

                if (name.equals("ID")) {
                    currentOwner.setId(checkForEmptyString(getText()));
                } else if (name.equals("DisplayName")) {
                    currentOwner.setDisplayName(checkForEmptyString(getText()));
                }
            }
        }

        private Integer parseInteger(String text) {
            text = checkForEmptyString(getText());
            if (text == null) return null;
            return Integer.parseInt(text);
        }
    }

    /**
     * Handler for parsing the get replication configuration response from Amazon S3. Sample HTTP
     * response is given below.
     *
     * <pre>
     * <ReplicationConfiguration>
     * 	<Rule>
     *   	<ID>replication-rule-1-1421862858808</ID>
     *   	<Prefix>testPrefix1</Prefix>
     *   	<Status>Enabled</Status>
     *   	<Destination>
     *       	<Bucket>bucketARN</Bucket>
     *   	</Destination>
     * </Rule>
     * <Rule>
     *   	<ID>replication-rule-2-1421862858808</ID>
     *   	<Prefix>testPrefix2</Prefix>
     *   	<Status>Disabled</Status>
     *   	<Destination>
     *       	<Bucket>arn:aws:s3:::bucket-dest-replication-integ-test-1421862858808</Bucket>
     *   	</Destination>
     * </Rule>
     * </ReplicationConfiguration>
     * </pre>
     */
    public static class BucketReplicationConfigurationHandler extends AbstractHandler {

        private final BucketReplicationConfiguration bucketReplicationConfiguration =
                new BucketReplicationConfiguration();
        private String currentRuleId;
        private ReplicationRule currentRule;
        private ReplicationDestinationConfig destinationConfig;
        private AccessControlTranslation accessControlTranslation;
        private EncryptionConfiguration encryptionConfiguration;
        private SourceSelectionCriteria sourceSelectionCriteria;
        private SseKmsEncryptedObjects sseKmsEncryptedObjects;
        private static final String REPLICATION_CONFIG = "ReplicationConfiguration";
        private static final String ROLE = "Role";
        private static final String RULE = "Rule";
        private static final String DESTINATION = "Destination";
        private static final String ID = "ID";
        private static final String PREFIX = "Prefix";
        private static final String STATUS = "Status";
        private static final String BUCKET = "Bucket";
        private static final String STORAGECLASS = "StorageClass";
        private static final String ACCOUNT = "Account";
        private static final String ACCESS_CONTROL_TRANSLATION = "AccessControlTranslation";
        private static final String OWNER = "Owner";
        private static final String ENCRYPTION_CONFIGURATION = "EncryptionConfiguration";
        private static final String REPLICA_KMS_KEY_ID = "ReplicaKmsKeyID";
        private static final String SOURCE_SELECTION_CRITERIA = "SourceSelectionCriteria";
        private static final String SSE_KMS_ENCRYPTED_OBJECTS = "SseKmsEncryptedObjects";

        public BucketReplicationConfiguration getConfiguration() {
            return bucketReplicationConfiguration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in(REPLICATION_CONFIG)) {
                if (name.equals(RULE)) {
                    currentRule = new ReplicationRule();
                }
            } else if (in(REPLICATION_CONFIG, RULE)) {
                if (name.equals(DESTINATION)) {
                    destinationConfig = new ReplicationDestinationConfig();
                } else if (name.equals(SOURCE_SELECTION_CRITERIA)) {
                    sourceSelectionCriteria = new SourceSelectionCriteria();
                }
            } else if (in(REPLICATION_CONFIG, RULE, DESTINATION)) {
                if (name.equals(ACCESS_CONTROL_TRANSLATION)) {
                    accessControlTranslation = new AccessControlTranslation();
                } else if (name.equals(ENCRYPTION_CONFIGURATION)) {
                    encryptionConfiguration = new EncryptionConfiguration();
                }
            } else if (in(REPLICATION_CONFIG, RULE, SOURCE_SELECTION_CRITERIA)) {
                if (name.equals(SSE_KMS_ENCRYPTED_OBJECTS)) {
                    sseKmsEncryptedObjects = new SseKmsEncryptedObjects();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in(REPLICATION_CONFIG)) {
                if (name.equals(RULE)) {
                    bucketReplicationConfiguration.addRule(currentRuleId, currentRule);
                    currentRule = null;
                    currentRuleId = null;
                    destinationConfig = null;
                    sseKmsEncryptedObjects = null;
                    accessControlTranslation = null;
                    encryptionConfiguration = null;
                } else if (name.equals(ROLE)) {
                    bucketReplicationConfiguration.setRoleARN(getText());
                }
            } else if (in(REPLICATION_CONFIG, RULE)) {
                if (name.equals(ID)) {
                    currentRuleId = getText();
                } else if (name.equals(PREFIX)) {
                    currentRule.setPrefix(getText());
                } else if (name.equals(SOURCE_SELECTION_CRITERIA)) {
                    currentRule.setSourceSelectionCriteria(sourceSelectionCriteria);
                } else {
                    if (name.equals(STATUS)) {
                        currentRule.setStatus(getText());

                    } else if (name.equals(DESTINATION)) {
                        currentRule.setDestinationConfig(destinationConfig);
                    }
                }
            } else if (in(REPLICATION_CONFIG, RULE, SOURCE_SELECTION_CRITERIA)) {
                if (name.equals(SSE_KMS_ENCRYPTED_OBJECTS)) {
                    sourceSelectionCriteria.setSseKmsEncryptedObjects(sseKmsEncryptedObjects);
                }
            } else if (in(
                    REPLICATION_CONFIG,
                    RULE,
                    SOURCE_SELECTION_CRITERIA,
                    SSE_KMS_ENCRYPTED_OBJECTS)) {
                if (name.equals(STATUS)) {
                    sseKmsEncryptedObjects.setStatus(getText());
                }
            } else if (in(REPLICATION_CONFIG, RULE, DESTINATION)) {
                if (name.equals(BUCKET)) {
                    destinationConfig.setBucketARN(getText());
                } else if (name.equals(STORAGECLASS)) {
                    destinationConfig.setStorageClass(getText());
                } else if (name.equals(ACCOUNT)) {
                    destinationConfig.setAccount(getText());
                } else if (name.equals(ACCESS_CONTROL_TRANSLATION)) {
                    destinationConfig.setAccessControlTranslation(accessControlTranslation);
                } else if (name.equals(ENCRYPTION_CONFIGURATION)) {
                    destinationConfig.setEncryptionConfiguration(encryptionConfiguration);
                }
            } else if (in(REPLICATION_CONFIG, RULE, DESTINATION, ACCESS_CONTROL_TRANSLATION)) {
                if (name.equals(OWNER)) {
                    accessControlTranslation.setOwner(getText());
                }
            } else if (in(REPLICATION_CONFIG, RULE, DESTINATION, ENCRYPTION_CONFIGURATION)) {
                if (name.equals(REPLICA_KMS_KEY_ID)) {
                    encryptionConfiguration.setReplicaKmsKeyID(getText());
                }
            }
        }
    }

    public static class BucketTaggingConfigurationHandler extends AbstractHandler {

        private final BucketTaggingConfiguration configuration = new BucketTaggingConfiguration();

        private Map<String, String> currentTagSet;
        private String currentTagKey;
        private String currentTagValue;

        public BucketTaggingConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("Tagging")) {
                if (name.equals("TagSet")) {
                    currentTagSet = new HashMap<String, String>();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("Tagging")) {
                if (name.equals("TagSet")) {
                    configuration.getAllTagSets().add(new TagSet(currentTagSet));
                    currentTagSet = null;
                }
            } else if (in("Tagging", "TagSet")) {
                if (name.equals("Tag")) {
                    if (currentTagKey != null && currentTagValue != null) {
                        currentTagSet.put(currentTagKey, currentTagValue);
                    }
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in("Tagging", "TagSet", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            }
        }
    }

    /**
     * Handler for unmarshalling the response from GET Object Tagging.
     *
     * <p><Tagging> <TagSet> <Tag> <Key>Foo</Key> <Value>1</Value> </Tag> <Tag> <Key>Bar</Key>
     * <Value>2</Value> </Tag> <Tag> <Key>Baz</Key> <Value>3</Value> </Tag> </TagSet> </Tagging>
     */
    public static class GetObjectTaggingHandler extends AbstractHandler {
        private GetObjectTaggingResult getObjectTaggingResult;
        private List<Tag> tagSet;
        private String currentTagValue;
        private String currentTagKey;

        public GetObjectTaggingResult getResult() {
            return getObjectTaggingResult;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {
            if (in("Tagging")) {
                if (name.equals("TagSet")) {
                    tagSet = new ArrayList<Tag>();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("Tagging")) {
                if (name.equals("TagSet")) {
                    getObjectTaggingResult = new GetObjectTaggingResult(tagSet);
                    tagSet = null;
                }
            }
            if (in("Tagging", "TagSet")) {
                if (name.equals("Tag")) {
                    tagSet.add(new Tag(currentTagKey, currentTagValue));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in("Tagging", "TagSet", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            }
        }
    }

    /*
       HTTP/1.1 200 OK
       x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
       x-amz-request-id: 656c76696e6727732072657175657374
       Date: Tue, 20 Sep 2012 20:34:56 GMT
       Content-Type: application/xml
       Transfer-Encoding: chunked
       Connection: keep-alive
       Server: AmazonS3

       <?xml version="1.0" encoding="UTF-8"?>
       <DeleteResult>
           <Deleted>
              <Key>Key</Key>
              <VersionId>Version</VersionId>
           </Deleted>
           <Error>
              <Key>Key</Key>
              <VersionId>Version</VersionId>
              <Code>Code</Code>
              <Message>Message</Message>
           </Error>
           <Deleted>
              <Key>Key</Key>
              <DeleteMarker>true</DeleteMarker>
              <DeleteMarkerVersionId>Version</DeleteMarkerVersionId>
           </Deleted>
       </DeleteResult>
    */
    public static class DeleteObjectsHandler extends AbstractHandler {

        private final DeleteObjectsResponse response = new DeleteObjectsResponse();

        private DeletedObject currentDeletedObject = null;
        private DeleteError currentError = null;

        public DeleteObjectsResponse getDeleteObjectResult() {
            return response;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("DeleteResult")) {
                if (name.equals("Deleted")) {
                    currentDeletedObject = new DeletedObject();
                } else if (name.equals("Error")) {
                    currentError = new DeleteError();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("DeleteResult")) {
                if (name.equals("Deleted")) {
                    response.getDeletedObjects().add(currentDeletedObject);
                    currentDeletedObject = null;
                } else if (name.equals("Error")) {
                    response.getErrors().add(currentError);
                    currentError = null;
                }
            } else if (in("DeleteResult", "Deleted")) {
                if (name.equals("Key")) {
                    currentDeletedObject.setKey(getText());

                } else if (name.equals("VersionId")) {
                    currentDeletedObject.setVersionId(getText());

                } else if (name.equals("DeleteMarker")) {
                    currentDeletedObject.setDeleteMarker(getText().equals("true"));

                } else if (name.equals("DeleteMarkerVersionId")) {
                    currentDeletedObject.setDeleteMarkerVersionId(getText());
                }
            } else if (in("DeleteResult", "Error")) {
                if (name.equals("Key")) {
                    currentError.setKey(getText());

                } else if (name.equals("VersionId")) {
                    currentError.setVersionId(getText());

                } else if (name.equals("Code")) {
                    currentError.setCode(getText());

                } else if (name.equals("Message")) {
                    currentError.setMessage(getText());
                }
            }
        }
    }

    /**
     * HTTP/1.1 200 OK x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
     * x-amz-request-id: 656c76696e6727732072657175657374 Date: Tue, 20 Sep 2012 20:34:56 GMT
     * Content-Length: xxx Connection: keep-alive Server: AmazonS3
     *
     * <p><LifecycleConfiguration> <Rule> <ID>logs-rule</ID> <Prefix>logs/</Prefix>
     * <Status>Enabled</Status> <Filter> <Prefix>logs/</Prefix> <Tag> <Key>key1</Key>
     * <Value>value1</Value> </Tag> <And> <Prefix>logs/</Prefix> <Tag> <Key>key1</Key>
     * <Value>value1</Value> </Tag> <Tag> <Key>key1</Key> <Value>value1</Value> </Tag> </And>
     * </Filter> <Transition> <Days>30</Days> <StorageClass>STANDARD_IA</StorageClass> </Transition>
     * <Transition> <Days>90</Days> <StorageClass>GLACIER</StorageClass> </Transition> <Expiration>
     * <Days>365</Days> </Expiration> <NoncurrentVersionTransition>
     * <NoncurrentDays>7</NoncurrentDays> <StorageClass>STANDARD_IA</StorageClass>
     * </NoncurrentVersionTransition> <NoncurrentVersionTransition>
     * <NoncurrentDays>14</NoncurrentDays> <StorageClass>GLACIER</StorageClass>
     * </NoncurrentVersionTransition> <NoncurrentVersionExpiration>
     * <NoncurrentDays>365</NoncurrentDays> </NoncurrentVersionExpiration> </Rule> <Rule>
     * <ID>image-rule</ID> <Prefix>image/</Prefix> <Status>Enabled</Status> <Transition>
     * <Date>2012-12-31T00:00:00.000Z</Date> <StorageClass>GLACIER</StorageClass> </Transition>
     * <Expiration> <Date>2020-12-31T00:00:00.000Z</Date> </Expiration>
     * <AbortIncompleteMultipartUpload> <DaysAfterInitiation>10</DaysAfterInitiation>
     * </AbortIncompleteMultipartUpload> </Rule> </LifecycleConfiguration>
     */
    public static class BucketLifecycleConfigurationHandler extends AbstractHandler {

        private final BucketLifecycleConfiguration configuration =
                new BucketLifecycleConfiguration(new ArrayList<Rule>());

        private Rule currentRule;
        private Transition currentTransition;
        private NoncurrentVersionTransition currentNcvTransition;
        private AbortIncompleteMultipartUpload abortIncompleteMultipartUpload;
        private LifecycleFilter currentFilter;
        private List<LifecycleFilterPredicate> andOperandsList;
        private String currentTagKey;
        private String currentTagValue;

        public BucketLifecycleConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("LifecycleConfiguration")) {
                if (name.equals("Rule")) {
                    currentRule = new Rule();
                }
            } else if (in("LifecycleConfiguration", "Rule")) {
                if (name.equals("Transition")) {
                    currentTransition = new Transition();
                } else if (name.equals("NoncurrentVersionTransition")) {
                    currentNcvTransition = new NoncurrentVersionTransition();
                } else if (name.equals("AbortIncompleteMultipartUpload")) {
                    abortIncompleteMultipartUpload = new AbortIncompleteMultipartUpload();
                } else if (name.equals("Filter")) {
                    currentFilter = new LifecycleFilter();
                }
            } else if (in("LifecycleConfiguration", "Rule", "Filter")) {
                if (name.equals("And")) {
                    andOperandsList = new ArrayList<LifecycleFilterPredicate>();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("LifecycleConfiguration")) {
                if (name.equals("Rule")) {
                    configuration.getRules().add(currentRule);
                    currentRule = null;
                }
            } else if (in("LifecycleConfiguration", "Rule")) {
                if (name.equals("ID")) {
                    currentRule.setId(getText());

                } else if (name.equals("Prefix")) {
                    currentRule.setPrefix(getText());

                } else if (name.equals("Status")) {
                    currentRule.setStatus(getText());

                } else if (name.equals("Transition")) {
                    currentRule.addTransition(currentTransition);
                    currentTransition = null;

                } else if (name.equals("NoncurrentVersionTransition")) {
                    currentRule.addNoncurrentVersionTransition(currentNcvTransition);
                    currentNcvTransition = null;
                } else if (name.equals("AbortIncompleteMultipartUpload")) {
                    currentRule.setAbortIncompleteMultipartUpload(abortIncompleteMultipartUpload);
                    abortIncompleteMultipartUpload = null;
                } else if (name.equals("Filter")) {
                    currentRule.setFilter(currentFilter);
                    currentFilter = null;
                }
            } else if (in("LifecycleConfiguration", "Rule", "Expiration")) {
                if (name.equals("Date")) {
                    currentRule.setExpirationDate(ServiceUtils.parseIso8601Date(getText()));
                } else if (name.equals("Days")) {
                    currentRule.setExpirationInDays(Integer.parseInt(getText()));
                } else if (name.equals("ExpiredObjectDeleteMarker")) {
                    if ("true".equals(getText())) {
                        currentRule.setExpiredObjectDeleteMarker(true);
                    }
                }
            } else if (in("LifecycleConfiguration", "Rule", "Transition")) {
                if (name.equals("StorageClass")) {
                    currentTransition.setStorageClass(getText());
                } else if (name.equals("Date")) {
                    currentTransition.setDate(ServiceUtils.parseIso8601Date(getText()));

                } else if (name.equals("Days")) {
                    currentTransition.setDays(Integer.parseInt(getText()));
                }
            } else if (in("LifecycleConfiguration", "Rule", "NoncurrentVersionExpiration")) {
                if (name.equals("NoncurrentDays")) {
                    currentRule.setNoncurrentVersionExpirationInDays(Integer.parseInt(getText()));
                }
            } else if (in("LifecycleConfiguration", "Rule", "NoncurrentVersionTransition")) {
                if (name.equals("StorageClass")) {
                    currentNcvTransition.setStorageClass(getText());
                } else if (name.equals("NoncurrentDays")) {
                    currentNcvTransition.setDays(Integer.parseInt(getText()));
                }
            } else if (in("LifecycleConfiguration", "Rule", "AbortIncompleteMultipartUpload")) {
                if (name.equals("DaysAfterInitiation")) {
                    abortIncompleteMultipartUpload.setDaysAfterInitiation(
                            Integer.parseInt(getText()));
                }
            } else if (in("LifecycleConfiguration", "Rule", "Filter")) {
                if (name.equals("Prefix")) {
                    currentFilter.setPredicate(new LifecyclePrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    currentFilter.setPredicate(
                            new LifecycleTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                } else if (name.equals("And")) {
                    currentFilter.setPredicate(new LifecycleAndOperator(andOperandsList));
                    andOperandsList = null;
                }
            } else if (in("LifecycleConfiguration", "Rule", "Filter", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in("LifecycleConfiguration", "Rule", "Filter", "And")) {
                if (name.equals("Prefix")) {
                    andOperandsList.add(new LifecyclePrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    andOperandsList.add(
                            new LifecycleTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in("LifecycleConfiguration", "Rule", "Filter", "And", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            }
        }
    }

    /*
    HTTP/1.1 200 OK
    x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
    x-amz-request-id: 656c76696e6727732072657175657374
    Date: Tue, 20 Sep 2011 20:34:56 GMT
    Content-Length: Some Length
    Connection: keep-alive
    Server: AmazonS3
    <CORSConfiguration>
       <CORSRule>
         <AllowedOrigin>http://www.foobar.com</AllowedOrigin>
         <AllowedMethod>GET</AllowedMethod>
         <MaxAgeSeconds>3000</MaxAgeSec>
         <ExposeHeader>x-amz-server-side-encryption</ExposeHeader>
       </CORSRule>
    </CORSConfiguration>
    */
    public static class BucketCrossOriginConfigurationHandler extends AbstractHandler {

        private final BucketCrossOriginConfiguration configuration =
                new BucketCrossOriginConfiguration(new ArrayList<CORSRule>());

        private CORSRule currentRule;
        private List<AllowedMethods> allowedMethods = null;
        private List<String> allowedOrigins = null;
        private List<String> exposedHeaders = null;
        private List<String> allowedHeaders = null;

        public BucketCrossOriginConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("CORSConfiguration")) {
                if (name.equals("CORSRule")) {
                    currentRule = new CORSRule();
                }
            } else if (in("CORSConfiguration", "CORSRule")) {
                if (name.equals("AllowedOrigin")) {
                    if (allowedOrigins == null) {
                        allowedOrigins = new ArrayList<String>();
                    }
                } else if (name.equals("AllowedMethod")) {
                    if (allowedMethods == null) {
                        allowedMethods = new ArrayList<AllowedMethods>();
                    }
                } else if (name.equals("ExposeHeader")) {
                    if (exposedHeaders == null) {
                        exposedHeaders = new ArrayList<String>();
                    }
                } else if (name.equals("AllowedHeader")) {
                    if (allowedHeaders == null) {
                        allowedHeaders = new LinkedList<String>();
                    }
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("CORSConfiguration")) {
                if (name.equals("CORSRule")) {
                    currentRule.setAllowedHeaders(allowedHeaders);
                    currentRule.setAllowedMethods(allowedMethods);
                    currentRule.setAllowedOrigins(allowedOrigins);
                    currentRule.setExposedHeaders(exposedHeaders);
                    allowedHeaders = null;
                    allowedMethods = null;
                    allowedOrigins = null;
                    exposedHeaders = null;

                    configuration.getRules().add(currentRule);
                    currentRule = null;
                }
            } else if (in("CORSConfiguration", "CORSRule")) {
                if (name.equals("ID")) {
                    currentRule.setId(getText());

                } else if (name.equals("AllowedOrigin")) {
                    allowedOrigins.add(getText());

                } else if (name.equals("AllowedMethod")) {
                    allowedMethods.add(AllowedMethods.fromValue(getText()));

                } else if (name.equals("MaxAgeSeconds")) {
                    currentRule.setMaxAgeSeconds(Integer.parseInt(getText()));

                } else if (name.equals("ExposeHeader")) {
                    exposedHeaders.add(getText());

                } else if (name.equals("AllowedHeader")) {
                    allowedHeaders.add(getText());
                }
            }
        }
    }

    /*
      HTTP/1.1 200 OK
      x-amz-id-2: ITnGT1y4RyTmXa3rPi4hklTXouTf0hccUjo0iCPjz6FnfIutBj3M7fPGlWO2SEWp
      x-amz-request-id: 51991C342C575321
      Date: Wed, 14 May 2014 02:11:22 GMT
      Server: AmazonS3
      Content-Length: ...

     <?xml version="1.0" encoding="UTF-8"?>
     <MetricsConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Id>metrics-id</Id>
       <Filter>
       <!-- A filter should have only one of Prefix, Tag or And predicate. ->
         <Prefix>prefix</Prefix>
         <Tag>
             <Key>Project</Key>
             <Value>Foo</Value>
         </Tag>
         <And>
           <Prefix>documents/</Prefix>
           <Tag>
             <Key>foo</Key>
             <Value>bar</Value>
           </Tag>
         </And>
       </Filter>
     </MetricsConfiguration>
    */
    public static class GetBucketMetricsConfigurationHandler extends AbstractHandler {

        private final MetricsConfiguration configuration = new MetricsConfiguration();

        private MetricsFilter filter;
        private List<MetricsFilterPredicate> andOperandsList;
        private String currentTagKey;
        private String currentTagValue;

        public GetBucketMetricsConfigurationResult getResult() {
            return new GetBucketMetricsConfigurationResult()
                    .withMetricsConfiguration(configuration);
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("MetricsConfiguration")) {
                if (name.equals("Filter")) {
                    filter = new MetricsFilter();
                }

            } else if (in("MetricsConfiguration", "Filter")) {
                if (name.equals("And")) {
                    andOperandsList = new ArrayList<MetricsFilterPredicate>();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("MetricsConfiguration")) {
                if (name.equals("Id")) {
                    configuration.setId(getText());
                } else if (name.equals("Filter")) {
                    configuration.setFilter(filter);
                    filter = null;
                }
            } else if (in("MetricsConfiguration", "Filter")) {
                if (name.equals("Prefix")) {
                    filter.setPredicate(new MetricsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    filter.setPredicate(
                            new MetricsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                } else if (name.equals("And")) {
                    filter.setPredicate(new MetricsAndOperator(andOperandsList));
                    andOperandsList = null;
                }
            } else if (in("MetricsConfiguration", "Filter", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in("MetricsConfiguration", "Filter", "And")) {
                if (name.equals("Prefix")) {
                    andOperandsList.add(new MetricsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    andOperandsList.add(
                            new MetricsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in("MetricsConfiguration", "Filter", "And", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            }
        }
    }

    /*
        HTTP/1.1 200 OK
        x-amz-id-2: ITnGT1y4RyTmXa3rPi4hklTXouTf0hccUjo0iCPjz6FnfIutBj3M7fPGlWO2SEWp
        x-amz-request-id: 51991C342C575321
        Date: Wed, 14 May 2014 02:11:22 GMT
        Server: AmazonS3
        Content-Length: ...

        <ListMetricsConfigurationsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <MetricsConfiguration>
            ...
          </MetricsConfiguration>
          <IsTruncated>true</IsTruncated>
          <ContinuationToken>token1</ContinuationToken>
          <NextContinuationToken>token2</NextContinuationToken>
        </ListMetricsConfigurationsResult>
    */
    public static class ListBucketMetricsConfigurationsHandler extends AbstractHandler {

        private final ListBucketMetricsConfigurationsResult result =
                new ListBucketMetricsConfigurationsResult();

        private MetricsConfiguration currentConfiguration;
        private MetricsFilter currentFilter;
        private List<MetricsFilterPredicate> andOperandsList;
        private String currentTagKey;
        private String currentTagValue;

        public ListBucketMetricsConfigurationsResult getResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListMetricsConfigurationsResult")) {
                if (name.equals("MetricsConfiguration")) {
                    currentConfiguration = new MetricsConfiguration();
                }

            } else if (in("ListMetricsConfigurationsResult", "MetricsConfiguration")) {
                if (name.equals("Filter")) {
                    currentFilter = new MetricsFilter();
                }

            } else if (in("ListMetricsConfigurationsResult", "MetricsConfiguration", "Filter")) {
                if (name.equals("And")) {
                    andOperandsList = new ArrayList<MetricsFilterPredicate>();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {

            if (in("ListMetricsConfigurationsResult")) {
                if (name.equals("MetricsConfiguration")) {
                    if (result.getMetricsConfigurationList() == null) {
                        result.setMetricsConfigurationList(new ArrayList<MetricsConfiguration>());
                    }
                    result.getMetricsConfigurationList().add(currentConfiguration);
                    currentConfiguration = null;
                } else if (name.equals("IsTruncated")) {
                    result.setTruncated("true".equals(getText()));
                } else if (name.equals("ContinuationToken")) {
                    result.setContinuationToken(getText());
                } else if (name.equals("NextContinuationToken")) {
                    result.setNextContinuationToken(getText());
                }
            } else if (in("ListMetricsConfigurationsResult", "MetricsConfiguration")) {
                if (name.equals("Id")) {
                    currentConfiguration.setId(getText());
                } else if (name.equals("Filter")) {
                    currentConfiguration.setFilter(currentFilter);
                    currentFilter = null;
                }
            } else if (in("ListMetricsConfigurationsResult", "MetricsConfiguration", "Filter")) {
                if (name.equals("Prefix")) {
                    currentFilter.setPredicate(new MetricsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    currentFilter.setPredicate(
                            new MetricsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                } else if (name.equals("And")) {
                    currentFilter.setPredicate(new MetricsAndOperator(andOperandsList));
                    andOperandsList = null;
                }
            } else if (in(
                    "ListMetricsConfigurationsResult", "MetricsConfiguration", "Filter", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in(
                    "ListMetricsConfigurationsResult", "MetricsConfiguration", "Filter", "And")) {
                if (name.equals("Prefix")) {
                    andOperandsList.add(new MetricsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    andOperandsList.add(
                            new MetricsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in(
                    "ListMetricsConfigurationsResult",
                    "MetricsConfiguration",
                    "Filter",
                    "And",
                    "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            }
        }
    }

    /*
        HTTP/1.1 200 OK
        x-amz-id-2: ITnGT1y4RyTmXa3rPi4hklTXouTf0hccUjo0iCPjz6FnfIutBj3M7fPGlWO2SEWp
        x-amz-request-id: 51991C342C575321
        Date: Wed, 14 May 2014 02:11:22 GMT
        Server: AmazonS3
        Content-Length: ...

       <?xml version="1.0" encoding="UTF-8"?>
       <AnalyticsConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Id>XXX</Id>
          <Filter>
            <And>
              <Prefix>documents/</Prefix>
              <Tag>
                <Key>foo</Key>
                <Value>bar</Value>
              </Tag>
            </And>
          </Filter>
          <StorageClassAnalysis>
            <DataExport>
              <OutputSchemaVersion>1</OutputSchemaVersion>
              <Destination>
                <S3BucketDestination>
                  <Format>CSV</Format>
                  <BucketAccountId>123456789</BucketAccountId>
                  <Bucket>destination-bucket</Bucket>
                  <Prefix>destination-prefix</Prefix>
                </S3BucketDestination>
              </Destination>
            </DataExport>
          </StorageClassAnalysis>
       </AnalyticsConfiguration>
    */
    public static class GetBucketAnalyticsConfigurationHandler extends AbstractHandler {

        private final AnalyticsConfiguration configuration = new AnalyticsConfiguration();

        private AnalyticsFilter filter;
        private List<AnalyticsFilterPredicate> andOperandsList;
        private StorageClassAnalysis storageClassAnalysis;
        private StorageClassAnalysisDataExport dataExport;
        private AnalyticsExportDestination destination;
        private AnalyticsS3BucketDestination s3BucketDestination;

        private String currentTagKey;
        private String currentTagValue;

        public GetBucketAnalyticsConfigurationResult getResult() {
            return new GetBucketAnalyticsConfigurationResult()
                    .withAnalyticsConfiguration(configuration);
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("AnalyticsConfiguration")) {
                if (name.equals("Filter")) {
                    filter = new AnalyticsFilter();
                } else if (name.equals("StorageClassAnalysis")) {
                    storageClassAnalysis = new StorageClassAnalysis();
                }

            } else if (in("AnalyticsConfiguration", "Filter")) {
                if (name.equals("And")) {
                    andOperandsList = new ArrayList<AnalyticsFilterPredicate>();
                }

            } else if (in("AnalyticsConfiguration", "StorageClassAnalysis")) {
                if (name.equals("DataExport")) {
                    dataExport = new StorageClassAnalysisDataExport();
                }

            } else if (in("AnalyticsConfiguration", "StorageClassAnalysis", "DataExport")) {
                if (name.equals("Destination")) {
                    destination = new AnalyticsExportDestination();
                }

            } else if (in(
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    s3BucketDestination = new AnalyticsS3BucketDestination();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {
            if (in("AnalyticsConfiguration")) {
                if (name.equals("Id")) {
                    configuration.setId(getText());
                } else if (name.equals("Filter")) {
                    configuration.setFilter(filter);
                } else if (name.equals("StorageClassAnalysis")) {
                    configuration.setStorageClassAnalysis(storageClassAnalysis);
                }
            } else if (in("AnalyticsConfiguration", "Filter")) {
                if (name.equals("Prefix")) {
                    filter.setPredicate(new AnalyticsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    filter.setPredicate(
                            new AnalyticsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                } else if (name.equals("And")) {
                    filter.setPredicate(new AnalyticsAndOperator(andOperandsList));
                    andOperandsList = null;
                }
            } else if (in("AnalyticsConfiguration", "Filter", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in("AnalyticsConfiguration", "Filter", "And")) {
                if (name.equals("Prefix")) {
                    andOperandsList.add(new AnalyticsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    andOperandsList.add(
                            new AnalyticsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in("AnalyticsConfiguration", "Filter", "And", "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in("AnalyticsConfiguration", "StorageClassAnalysis")) {
                if (name.equals("DataExport")) {
                    storageClassAnalysis.setDataExport(dataExport);
                }
            } else if (in("AnalyticsConfiguration", "StorageClassAnalysis", "DataExport")) {
                if (name.equals("OutputSchemaVersion")) {
                    dataExport.setOutputSchemaVersion(getText());
                } else if (name.equals("Destination")) {
                    dataExport.setDestination(destination);
                }
            } else if (in(
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    destination.setS3BucketDestination(s3BucketDestination);
                }
            } else if (in(
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination",
                    "S3BucketDestination")) {
                if (name.equals("Format")) {
                    s3BucketDestination.setFormat(getText());
                } else if (name.equals("BucketAccountId")) {
                    s3BucketDestination.setBucketAccountId(getText());
                } else if (name.equals("Bucket")) {
                    s3BucketDestination.setBucketArn(getText());
                } else if (name.equals("Prefix")) {
                    s3BucketDestination.setPrefix(getText());
                }
            }
        }
    }

    /*
       HTTP/1.1 200 OK
       x-amz-id-2: ITnGT1y4RyTmXa3rPi4hklTXouTf0hccUjo0iCPjz6FnfIutBj3M7fPGlWO2SEWp
       x-amz-request-id: 51991C342C575321
       Date: Wed, 14 May 2014 02:11:22 GMT
       Server: AmazonS3
       Content-Length: ...

       <ListBucketAnalyticsConfigurationsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
         <AnalyticsConfiguration>
           ...
         </AnalyticsConfiguration>
         <IsTruncated>true</IsTruncated>
         <ContinuationToken>token1</ContinuationToken>
         <NextContinuationToken>token2</NextContinuationToken>
       </ListBucketAnalyticsConfigurationsResult>
    */
    public static class ListBucketAnalyticsConfigurationHandler extends AbstractHandler {

        private final ListBucketAnalyticsConfigurationsResult result =
                new ListBucketAnalyticsConfigurationsResult();

        private AnalyticsConfiguration currentConfiguration;
        private AnalyticsFilter currentFilter;
        private List<AnalyticsFilterPredicate> andOperandsList;
        private StorageClassAnalysis storageClassAnalysis;
        private StorageClassAnalysisDataExport dataExport;
        private AnalyticsExportDestination destination;
        private AnalyticsS3BucketDestination s3BucketDestination;
        private String currentTagKey;
        private String currentTagValue;

        public ListBucketAnalyticsConfigurationsResult getResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListBucketAnalyticsConfigurationsResult")) {
                if (name.equals("AnalyticsConfiguration")) {
                    currentConfiguration = new AnalyticsConfiguration();
                }

            } else if (in("ListBucketAnalyticsConfigurationsResult", "AnalyticsConfiguration")) {
                if (name.equals("Filter")) {
                    currentFilter = new AnalyticsFilter();
                } else if (name.equals("StorageClassAnalysis")) {
                    storageClassAnalysis = new StorageClassAnalysis();
                }

            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "Filter")) {
                if (name.equals("And")) {
                    andOperandsList = new ArrayList<AnalyticsFilterPredicate>();
                }

            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis")) {
                if (name.equals("DataExport")) {
                    dataExport = new StorageClassAnalysisDataExport();
                }

            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport")) {
                if (name.equals("Destination")) {
                    destination = new AnalyticsExportDestination();
                }

            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    s3BucketDestination = new AnalyticsS3BucketDestination();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {

            if (in("ListBucketAnalyticsConfigurationsResult")) {
                if (name.equals("AnalyticsConfiguration")) {
                    if (result.getAnalyticsConfigurationList() == null) {
                        result.setAnalyticsConfigurationList(
                                new ArrayList<AnalyticsConfiguration>());
                    }
                    result.getAnalyticsConfigurationList().add(currentConfiguration);
                    currentConfiguration = null;
                } else if (name.equals("IsTruncated")) {
                    result.setTruncated("true".equals(getText()));
                } else if (name.equals("ContinuationToken")) {
                    result.setContinuationToken(getText());
                } else if (name.equals("NextContinuationToken")) {
                    result.setNextContinuationToken(getText());
                }
            } else if (in("ListBucketAnalyticsConfigurationsResult", "AnalyticsConfiguration")) {
                if (name.equals("Id")) {
                    currentConfiguration.setId(getText());
                } else if (name.equals("Filter")) {
                    currentConfiguration.setFilter(currentFilter);
                } else if (name.equals("StorageClassAnalysis")) {
                    currentConfiguration.setStorageClassAnalysis(storageClassAnalysis);
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "Filter")) {
                if (name.equals("Prefix")) {
                    currentFilter.setPredicate(new AnalyticsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    currentFilter.setPredicate(
                            new AnalyticsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                } else if (name.equals("And")) {
                    currentFilter.setPredicate(new AnalyticsAndOperator(andOperandsList));
                    andOperandsList = null;
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "Filter",
                    "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "Filter",
                    "And")) {
                if (name.equals("Prefix")) {
                    andOperandsList.add(new AnalyticsPrefixPredicate(getText()));
                } else if (name.equals("Tag")) {
                    andOperandsList.add(
                            new AnalyticsTagPredicate(new Tag(currentTagKey, currentTagValue)));
                    currentTagKey = null;
                    currentTagValue = null;
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "Filter",
                    "And",
                    "Tag")) {
                if (name.equals("Key")) {
                    currentTagKey = getText();
                } else if (name.equals("Value")) {
                    currentTagValue = getText();
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis")) {
                if (name.equals("DataExport")) {
                    storageClassAnalysis.setDataExport(dataExport);
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport")) {
                if (name.equals("OutputSchemaVersion")) {
                    dataExport.setOutputSchemaVersion(getText());
                } else if (name.equals("Destination")) {
                    dataExport.setDestination(destination);
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    destination.setS3BucketDestination(s3BucketDestination);
                }
            } else if (in(
                    "ListBucketAnalyticsConfigurationsResult",
                    "AnalyticsConfiguration",
                    "StorageClassAnalysis",
                    "DataExport",
                    "Destination",
                    "S3BucketDestination")) {
                if (name.equals("Format")) {
                    s3BucketDestination.setFormat(getText());
                } else if (name.equals("BucketAccountId")) {
                    s3BucketDestination.setBucketAccountId(getText());
                } else if (name.equals("Bucket")) {
                    s3BucketDestination.setBucketArn(getText());
                } else if (name.equals("Prefix")) {
                    s3BucketDestination.setPrefix(getText());
                }
            }
        }
    }

    /*
        HTTP/1.1 200 OK
        x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
        x-amz-request-id: 656c76696e6727732072657175657374
        Date: Tue, 20 Sep 2012 20:34:56 GMT
        Content-Length: xxx
        Connection: keep-alive
        Server: AmazonS3

       <InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Destination>
             <S3BucketDestination>
                <AccountId>A2OCNCIEQW9MSG</AccountId>
                <Bucket>s3-object-inventory-list-gamma-us-east-1</Bucket>
                <Format>CSV</Format>
                <Prefix>string</Prefix>
             </S3BucketDestination>
          </Destination>
          <IsEnabled>true</IsEnabled>
          <Filter>
             <Prefix>string</Prefix>
          </Filter>
          <Id>configId</Id>
          <IncludedObjectVersions>All</IncludedObjectVersions>
          <OptionalFields>
             <Field>Size</Field>
             <Field>LastModifiedDate</Field>
             <Field>StorageClass</Field>
             <Field>ETag</Field>
             <Field>IsMultipartUploaded</Field>
             <Field>ReplicationStatus</Field>
          </OptionalFields>
          <Schedule>
             <Frequency>Daily</Frequency>
          </Schedule>
       </InventoryConfiguration>
    */
    public static class GetBucketInventoryConfigurationHandler extends AbstractHandler {

        public static final String SSE_S3 = "SSE-S3";
        public static final String SSE_KMS = "SSE-KMS";
        private final GetBucketInventoryConfigurationResult result =
                new GetBucketInventoryConfigurationResult();
        private final InventoryConfiguration configuration = new InventoryConfiguration();

        private List<String> optionalFields;
        private InventoryDestination inventoryDestination;
        private InventoryFilter filter;
        private InventoryS3BucketDestination s3BucketDestination;
        private InventorySchedule inventorySchedule;

        public GetBucketInventoryConfigurationResult getResult() {
            return result.withInventoryConfiguration(configuration);
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("InventoryConfiguration")) {
                if (name.equals("Destination")) {
                    inventoryDestination = new InventoryDestination();
                } else if (name.equals("Filter")) {
                    filter = new InventoryFilter();
                } else if (name.equals("Schedule")) {
                    inventorySchedule = new InventorySchedule();
                } else if (name.equals("OptionalFields")) {
                    optionalFields = new ArrayList<String>();
                }

            } else if (in("InventoryConfiguration", "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    s3BucketDestination = new InventoryS3BucketDestination();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {

            if (in("InventoryConfiguration")) {
                if (name.equals("Id")) {
                    configuration.setId(getText());

                } else if (name.equals("Destination")) {
                    configuration.setDestination(inventoryDestination);
                    inventoryDestination = null;

                } else if (name.equals("IsEnabled")) {
                    configuration.setEnabled("true".equals(getText()));

                } else if (name.equals("Filter")) {
                    configuration.setInventoryFilter(filter);
                    filter = null;

                } else if (name.equals("IncludedObjectVersions")) {
                    configuration.setIncludedObjectVersions(getText());

                } else if (name.equals("Schedule")) {
                    configuration.setSchedule(inventorySchedule);
                    inventorySchedule = null;

                } else if (name.equals("OptionalFields")) {
                    configuration.setOptionalFields(optionalFields);
                    optionalFields = null;
                }

            } else if (in("InventoryConfiguration", "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    inventoryDestination.setS3BucketDestination(s3BucketDestination);
                    s3BucketDestination = null;
                }

            } else if (in("InventoryConfiguration", "Destination", "S3BucketDestination")) {
                if (name.equals("AccountId")) {
                    s3BucketDestination.setAccountId(getText());
                } else if (name.equals("Bucket")) {
                    s3BucketDestination.setBucketArn(getText());
                } else if (name.equals("Format")) {
                    s3BucketDestination.setFormat(getText());
                } else if (name.equals("Prefix")) {
                    s3BucketDestination.setPrefix(getText());
                }
            } else if (in(
                    "InventoryConfiguration", "Destination", "S3BucketDestination", "Encryption")) {
                if (name.equals(SSE_S3)) {
                    s3BucketDestination.setEncryption(new ServerSideEncryptionS3());
                } else if (name.equals(SSE_KMS)) {
                    ServerSideEncryptionKMS kmsEncryption =
                            new ServerSideEncryptionKMS().withKeyId(getText());
                    s3BucketDestination.setEncryption(kmsEncryption);
                }
            } else if (in("InventoryConfiguration", "Filter")) {
                if (name.equals("Prefix")) {
                    filter.setPredicate(new InventoryPrefixPredicate(getText()));
                }

            } else if (in("InventoryConfiguration", "Schedule")) {
                if (name.equals("Frequency")) {
                    inventorySchedule.setFrequency(getText());
                }

            } else if (in("InventoryConfiguration", "OptionalFields")) {
                if (name.equals("Field")) {
                    optionalFields.add(getText());
                }
            }
        }
    }

    /*
           HTTP/1.1 200 OK
           x-amz-id-2: ITnGT1y4RyTmXa3rPi4hklTXouTf0hccUjo0iCPjz6FnfIutBj3M7fPGlWO2SEWp
           x-amz-request-id: 51991C342C575321
           Date: Wed, 14 May 2014 02:11:22 GMT
           Server: AmazonS3
           Content-Length: ...

           <ListInventoryConfigurationsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <InventoryConfiguration>
               ...
             </InventoryConfiguration>
             <InventoryConfiguration>
               ...
             </InventoryConfiguration>
             <IsTruncated>true</IsTruncated>
             <ContinuationToken>token1</ContinuationToken>
             <NextContinuationToken>token2</NextContinuationToken>
           </ListInventoryConfigurationsResult>
    */
    public static class ListBucketInventoryConfigurationsHandler extends AbstractHandler {

        private final ListBucketInventoryConfigurationsResult result =
                new ListBucketInventoryConfigurationsResult();

        private InventoryConfiguration currentConfiguration;
        private List<String> currentOptionalFieldsList;
        private InventoryDestination currentDestination;
        private InventoryFilter currentFilter;
        private InventoryS3BucketDestination currentS3BucketDestination;
        private InventorySchedule currentSchedule;

        public ListBucketInventoryConfigurationsResult getResult() {
            return result;
        }

        @Override
        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {

            if (in("ListInventoryConfigurationsResult")) {
                if (name.equals("InventoryConfiguration")) {
                    currentConfiguration = new InventoryConfiguration();
                }

            } else if (in("ListInventoryConfigurationsResult", "InventoryConfiguration")) {
                if (name.equals("Destination")) {
                    currentDestination = new InventoryDestination();
                } else if (name.equals("Filter")) {
                    currentFilter = new InventoryFilter();
                } else if (name.equals("Schedule")) {
                    currentSchedule = new InventorySchedule();
                } else if (name.equals("OptionalFields")) {
                    currentOptionalFieldsList = new ArrayList<String>();
                }

            } else if (in(
                    "ListInventoryConfigurationsResult", "InventoryConfiguration", "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    currentS3BucketDestination = new InventoryS3BucketDestination();
                }
            }
        }

        @Override
        protected void doEndElement(String uri, String name, String qName) {

            if (in("ListInventoryConfigurationsResult")) {
                if (name.equals("InventoryConfiguration")) {
                    if (result.getInventoryConfigurationList() == null) {
                        result.setInventoryConfigurationList(
                                new ArrayList<InventoryConfiguration>());
                    }
                    result.getInventoryConfigurationList().add(currentConfiguration);
                    currentConfiguration = null;
                } else if (name.equals("IsTruncated")) {
                    result.setTruncated("true".equals(getText()));
                } else if (name.equals("ContinuationToken")) {
                    result.setContinuationToken(getText());
                } else if (name.equals("NextContinuationToken")) {
                    result.setNextContinuationToken(getText());
                }
            } else if (in("ListInventoryConfigurationsResult", "InventoryConfiguration")) {
                if (name.equals("Id")) {
                    currentConfiguration.setId(getText());

                } else if (name.equals("Destination")) {
                    currentConfiguration.setDestination(currentDestination);
                    currentDestination = null;

                } else if (name.equals("IsEnabled")) {
                    currentConfiguration.setEnabled("true".equals(getText()));

                } else if (name.equals("Filter")) {
                    currentConfiguration.setInventoryFilter(currentFilter);
                    currentFilter = null;

                } else if (name.equals("IncludedObjectVersions")) {
                    currentConfiguration.setIncludedObjectVersions(getText());

                } else if (name.equals("Schedule")) {
                    currentConfiguration.setSchedule(currentSchedule);
                    currentSchedule = null;

                } else if (name.equals("OptionalFields")) {
                    currentConfiguration.setOptionalFields(currentOptionalFieldsList);
                    currentOptionalFieldsList = null;
                }

            } else if (in(
                    "ListInventoryConfigurationsResult", "InventoryConfiguration", "Destination")) {
                if (name.equals("S3BucketDestination")) {
                    currentDestination.setS3BucketDestination(currentS3BucketDestination);
                    currentS3BucketDestination = null;
                }

            } else if (in(
                    "ListInventoryConfigurationsResult",
                    "InventoryConfiguration",
                    "Destination",
                    "S3BucketDestination")) {
                if (name.equals("AccountId")) {
                    currentS3BucketDestination.setAccountId(getText());
                } else if (name.equals("Bucket")) {
                    currentS3BucketDestination.setBucketArn(getText());
                } else if (name.equals("Format")) {
                    currentS3BucketDestination.setFormat(getText());
                } else if (name.equals("Prefix")) {
                    currentS3BucketDestination.setPrefix(getText());
                }

            } else if (in(
                    "ListInventoryConfigurationsResult", "InventoryConfiguration", "Filter")) {
                if (name.equals("Prefix")) {
                    currentFilter.setPredicate(new InventoryPrefixPredicate(getText()));
                }

            } else if (in(
                    "ListInventoryConfigurationsResult", "InventoryConfiguration", "Schedule")) {
                if (name.equals("Frequency")) {
                    currentSchedule.setFrequency(getText());
                }

            } else if (in(
                    "ListInventoryConfigurationsResult",
                    "InventoryConfiguration",
                    "OptionalFields")) {
                if (name.equals("Field")) {
                    currentOptionalFieldsList.add(getText());
                }
            }
        }
    }

    private static String findAttributeValue(String qnameToFind, Attributes attrs) {

        for (int i = 0; i < attrs.getLength(); i++) {
            String qname = attrs.getQName(i);
            if (qname.trim().equalsIgnoreCase(qnameToFind.trim())) {
                return attrs.getValue(i);
            }
        }

        return null;
    }
}
