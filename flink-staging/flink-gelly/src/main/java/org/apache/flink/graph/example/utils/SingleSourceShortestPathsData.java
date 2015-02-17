package org.apache.flink.graph.example.utils;

public class SingleSourceShortestPathsData {

    public static final int NUM_VERTICES = 5;

    public static final Long SRC_VERTEX_ID = 1L;

    public static final String VERTICES =   "1,1.0\n" +
                                            "2,2.0\n" +
                                            "3,3.0\n" +
                                            "4,4.0\n" +
                                            "5,5.0";

    public static final String EDGES =  "1,2,12.0\n" +
                                        "1,3,13.0\n" +
                                        "2,3,23.0\n" +
                                        "3,4,34.0\n" +
                                        "3,5,35.0\n" +
                                        "4,5,45.0\n" +
                                        "5,1,51.0";

    public static final String RESULTED_SINGLE_SOURCE_SHORTEST_PATHS =  "1,0.0\n" +
                                                                        "2,12.0\n" +
                                                                        "3,13.0\n" +
                                                                        "4,47.0\n" +
                                                                        "5,48.0";

    private SingleSourceShortestPathsData() {}
}
