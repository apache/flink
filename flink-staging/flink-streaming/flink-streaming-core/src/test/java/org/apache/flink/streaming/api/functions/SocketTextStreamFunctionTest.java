/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SocketTextStreamFunctionTest {

    private static final String content = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras sagittis nisl non euismod fermentum. " +
            "Curabitur lacinia vehicula enim quis tristique. Suspendisse imperdiet arcu sed bibendum vulputate. " +
            "Sed vitae nisl vitae turpis dapibus lacinia in id elit. Integer lorem dolor, " +
            "porttitor ut nisi in, tincidunt sodales leo. Aliquam tristique dui sit amet odio bibendum, " +
            "et rutrum turpis auctor. Morbi sit amet mollis augue, ac rutrum velit. " +
            "Vestibulum suscipit finibus sapien, et congue enim laoreet consequat.\r\nInteger " +
            "aliquam metus iaculis risus hendrerit maximus. Suspendisse vestibulum nibh ac " +
            "mauris cursus molestie sit amet vel turpis. Nulla et posuere orci. " +
            "Aliquam dui quam, posuere vitae erat vitae, finibus commodo ipsum. Aliquam " +
            "eu dui quis arcu porttitor sollicitudin. Integer sodales finibus ullamcorper. " +
            "Praesent et felis tempor, laoreet libero eget, consequat nisl. " +
            "Aenean molestie rutrum lorem, ac cursus nisl dapibus vitae.\r\nQuisque sodales " +
            "dui et sem bibendum semper. Pellentesque luctus leo nec lacus euismod pellentesque. " +
            "Phasellus a metus dignissim risus auctor lacinia. Class " +
            "aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos" +
            ". Aenean consectetur bibendum imperdiet. Etiam dignissim rutrum enim, " +
            "non volutpat nisi condimentum sed. Quisque condimentum ultrices est sit amet " +
            "facilisis.\r\nUt vitae volutpat odio. Sed eget vestibulum libero, eu " +
            "tincidunt lorem. Nam pretium nulla nisl. Maecenas fringilla nunc ut turpis consectetur, " +
            "et fringilla sem placerat. Etiam nec scelerisque nisi, at sodales ligula. Aliquam " +
            "euismod faucibus egestas. Curabitur eget enim quam. Praesent convallis mattis lobortis. " +
            "Pellentesque a consectetur nisl. Duis molestie diam est. Nam a malesuada augue. " +
            "Vivamus enim massa, luctus ac elit ut, vestibulum laoreet nulla. " +
            "Curabitur pellentesque vel mi eget tempus. Donec cursus et leo quis viverra.\r\nIn ac imperdiet ex, " +
            "nec aliquet erat. Nullam sit amet enim in dolor finibus convallis id eu nibh. Fusce aliquam convallis orci aliquam.";

    private class TestServer implements Runnable{
        private volatile int port = 0;
        public int getPort(){
            return port;
        }
        private ServerSocket serverSocket;
        public TestServer() {
            try {
                serverSocket = new ServerSocket(port);
            } catch (IOException e) {
                e.printStackTrace();
            }
            port = serverSocket.getLocalPort();
        }

        @Override public void run() {
            try {
                while(true){
                    Socket clientSocket = serverSocket.accept();
                    try {
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
                        out.print(content);
                        out.flush();
                    }finally {
                        clientSocket.close();
                    }
                }
            } catch (IOException e) {
            }
        }

        public void stopServer() throws IOException {
            serverSocket.close();
        }
    }

    private TestServer testServer;

    @Before
    public void startTestServerSocket(){
        testServer = new TestServer();
        new Thread(testServer).start();
    }

    @After
    public void stopTestServerSocket() throws IOException {
        testServer.stopServer();
    }

    private void prepareTestForOld(char delimiter,List<String> actualList) throws Exception {
        SocketTextStreamFunction source = new SocketTextStreamFunction("localhost", testServer.getPort(), delimiter, 0);
        final ListSourceContext<String> flinkCollector = new ListSourceContext<String>(actualList);
        source.open(new Configuration());
        source.run(flinkCollector);
    }

    private void prepareTest(String delimiter,List<String> actualList) throws Exception {
        SocketTextStreamFunction source = new SocketTextStreamFunction("localhost", testServer.getPort(), delimiter, 0);
        final ListSourceContext<String> flinkCollector = new ListSourceContext<String>(actualList);
        source.open(new Configuration());
        source.run(flinkCollector);
    }

    private String restoreContentFromActual(String delimiter, List<String> actualList){
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (; i < actualList.size()-1; i++) {
            sb.append(actualList.get(i)+delimiter);
        }
        sb.append(actualList.get(i));
        return sb.toString().replace("\r","").replace("\n","\r\n");
    }

    @Test
    public void testNewLineDelimitedOldApiWithChar() throws Exception {
        List<String> actualList = new ArrayList<>();
        char delimiter = '\n';
        prepareTestForOld(delimiter, actualList);
        assertEquals(5, actualList.size());
        assertEquals(content,restoreContentFromActual(String.valueOf(delimiter),actualList));
    }

    @Test
    public void testCarriageDelimitedOldApiWithChar() throws Exception {
        List<String> actualList = new ArrayList<>();
        char delimiter = '\r';
        prepareTestForOld(delimiter,actualList);
        assertEquals(5, actualList.size());
        assertEquals(content,restoreContentFromActual(String.valueOf(delimiter), actualList));
    }

    @Test
    public void testNewLineDelimited() throws Exception {
        List<String> actualList = new ArrayList<>();
        char delimiter = '\n';
        prepareTestForOld(delimiter, actualList);
        assertEquals(5, actualList.size());
        assertEquals(content,restoreContentFromActual(String.valueOf(delimiter),actualList));
    }

    @Test
    public void testCarriageDelimited() throws Exception {
        List<String> actualList = new ArrayList<>();
        String delimiter = "\r";
        prepareTest(delimiter,actualList);
        assertEquals(5, actualList.size());
        assertTrue(actualList.get(1).indexOf('\n') != -1);
        assertEquals(content, restoreContentFromActual(delimiter, actualList));
    }

    @Test
    public void testWindowsLineEndDelimited() throws Exception {
        List<String> actualList = new ArrayList<>();
        String delimiter = "\r\n";
        prepareTest(delimiter,actualList);
        assertEquals(5, actualList.size());
        assertTrue(actualList.get(0).indexOf('\r') == -1);
        assertTrue(actualList.get(0).indexOf('\n') == -1);
        assertEquals(content, restoreContentFromActual(delimiter, actualList));
    }

    @Test
    public void testWindowsLineEndSuffixDelimited() throws Exception {
        List<String> actualList = new ArrayList<>();
        String delimiter = ".\r\n";
        prepareTest(delimiter,actualList);
        assertEquals(5, actualList.size());
        assertTrue(actualList.get(0).indexOf('\r') == -1);
        assertTrue(actualList.get(0).indexOf('\n') == -1);
        assertEquals(content, restoreContentFromActual(delimiter, actualList));
    }

    @Test
    public void testLongDelimited() throws Exception {
        List<String> actualList = new ArrayList<>();
        String delimiter = "Integer aliquam metus iaculis risus hendrerit maximus. " +
                "Suspendisse vestibulum nibh ac mauris cursus molestie sit amet vel turpis. " +
                "Nulla et posuere orci. Aliquam dui quam, posuere vitae erat vitae, " +
                "finibus commodo ipsum. Aliquam eu dui quis arcu porttitor sollicitudin. " +
                "Integer sodales finibus ullamcorper. Praesent et felis tempor, laoreet libero eget, " +
                "consequat nisl. Aenean molestie rutrum lorem, ac cursus nisl dapibus vitae."+
                "\r\nQuisque sodales dui et sem bibendum semper. Pellentesque luctus leo nec lacus euismod pellentesque. " +
                "Phasellus a metus dignissim risus auctor lacinia. Class " +
                "aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos" +
                ". Aenean consectetur bibendum imperdiet. Etiam dignissim rutrum enim, " +
                "non volutpat nisi condimentum sed. Quisque condimentum ultrices est sit amet " +
                "facilisis.\r\nUt vitae volutpat odio. Sed eget vestibulum libero, eu " +
                "tincidunt lorem. Nam pretium nulla nisl. Maecenas fringilla nunc ut turpis consectetur, " +
                "et fringilla sem placerat. Etiam nec scelerisque nisi, at sodales ligula. Aliquam " +
                "euismod faucibus egestas. Curabitur eget enim quam. Praesent convallis mattis lobortis. " +
                "Pellentesque a consectetur nisl. Duis molestie diam est. Nam a malesuada augue. " +
                "Vivamus enim massa, luctus ac elit ut, vestibulum laoreet nulla. " +
                "Curabitur pellentesque vel mi eget tempus. Donec cursus et leo quis viverra.";

        prepareTest(delimiter,actualList);
        assertEquals(content, restoreContentFromActual(delimiter, actualList));
        assertEquals(2, actualList.size());
        assertTrue(actualList.get(0).indexOf('\r') == -1);
        assertTrue(actualList.get(0).indexOf('\n') != -1);

    }

}
