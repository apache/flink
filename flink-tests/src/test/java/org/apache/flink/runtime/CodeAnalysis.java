package org.apache.flink.runtime;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * @Description
 * @Date 2020/7/30 
 *
 * @Author dugenkui
 **/
public class CodeAnalysis {
    public static long countCodeLine(String rootDir, List<String> postfix) throws IOException {
        File file = new File(rootDir);
        if (file.isDirectory()) {
            int totalCount = 0;
            for (File listFile : file.listFiles()) {
                totalCount += countCodeLine(listFile.getAbsolutePath(), postfix);
            }
            return totalCount;
        } else {
            String absolutePath = file.getAbsolutePath();
            if (postfix.stream().filter(ele -> absolutePath.endsWith(ele)).count() <= 0) {
                return 0;
            }
            return Files.lines(Paths.get(absolutePath)).count();
        }
    }
    public static void main(String[] args) throws IOException {
    	// 46w
        System.out.println(countCodeLine("/Users/moriushitorasakigake/github/flink/flink-runtime", Arrays.asList(".java")));
    }
}
