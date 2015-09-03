package org.apache.flink.examples.java.distcp;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * Created by zholudev on 27/08/15.
 * Representing a unit of copying
 */
public class FileCopyTask implements Serializable {
    private Path path;
    private String relativePath;

    public FileCopyTask(Path path, String relativePath) {
        if (StringUtils.isEmpty(relativePath)) {
            throw new IllegalArgumentException("Relative path should not be empty for: " + path);
        }
        this.path = path;
        this.relativePath = relativePath;
    }

    public Path getPath() {
        return path;
    }

    public String getRelativePath() {
        return relativePath;
    }

    @Override
    public String toString() {
        return "FileCopyTask{" +
                "path=" + path +
                ", relativePath='" + relativePath + '\'' +
                '}';
    }
}
