package org.apache.flink.table.examples.java.functions;

import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpMapper extends ScalarFunction {
    private static ConcurrentHashMap<String, String> ipMap = new ConcurrentHashMap<>();
    private Pattern ipPattern = Pattern.compile("\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d{1,5})?"); // Port
    private Connection conn;
    private Map<String, String> map;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        try {
            Class.forName(map.get("driverClass"));
            conn =
                    DriverManager.getConnection(
                            map.get("dbUrl"), map.get("userName"), map.get("passWord"));
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(map.get("query"));
            // Extract data from result set
            while (rs.next()) {
                ipMap.put(rs.getString(1), rs.getString(2));
            }
            ScheduledExecutorService scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("extractdata from ipMap table"));
            scheduler.scheduleWithFixedDelay(
                    () -> {
                        try {
                            ResultSet rs1 = stmt.executeQuery(map.get("query"));
                            synchronized (ipMap) {
                                ipMap.clear();
                                while (rs1.next()) {
                                    ipMap.put(rs1.getString(1), rs1.getString(2));
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    },
                    10,
                    10,
                    TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null) {
                conn.close();
            }
            throw e;
        }
    }

    public IpMapper(Object param) {
        super();
        map = (Map<String, String>) param;
    }

    public String eval(String value) {
        if (value == null) {
            return null;
        }
        Matcher matcher = ipPattern.matcher(value);
        if (matcher.find()) {
            String ip = matcher.group();
            synchronized (ipMap) {
                String newIp = ipMap.get(ip);
                if (newIp != null) {
                    value = value.replace(ip, newIp);
                }
            }
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }
}
