package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Utils used to extract tar.gz files and try to restore the origin permissions of files. */
@Internal
public class TarGzUtils {
    public static void extractTarGzFileWithPermissions(String tarGzFilePath, String targetPath)
            throws IOException {
        try (InputStream fi = Files.newInputStream(Paths.get(tarGzFilePath));
                InputStream bi = new BufferedInputStream(fi);
                InputStream gzi = new GzipCompressorInputStream(bi);
                ArchiveInputStream tari = new TarArchiveInputStream(gzi)) {
            boolean isUnix = DecompressUtils.isUnix();
            TarArchiveEntry entry;
            while ((entry = (TarArchiveEntry) tari.getNextEntry()) != null) {
                File file;
                if (entry.isDirectory()) {
                    file = new File(targetPath, entry.getName());
                    if (!file.exists()) {
                        if (!file.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + file.getAbsolutePath() + "failed!");
                        }
                    }
                } else {
                    file = new File(targetPath, entry.getName());
                    File parentDir = file.getParentFile();
                    if (!parentDir.exists()) {
                        if (!parentDir.mkdirs()) {
                            throw new IOException(
                                    "Create dir: " + file.getAbsolutePath() + "failed!");
                        }
                    }
                    if (file.createNewFile()) {
                        OutputStream output = new FileOutputStream(file);
                        byte[] buf = new byte[(int) entry.getSize()];
                        IOUtils.readFully(tari, buf, 0, buf.length);
                        IOUtils.copyBytes(new ByteArrayInputStream(buf), output);
                    } else {
                        throw new IOException("Create file: " + file.getAbsolutePath() + "failed!");
                    }
                }
                if (isUnix) {
                    int mode = entry.getMode();
                    if (mode != 0) {
                        DecompressUtils.setFilePermission(file, mode);
                    }
                }
            }
        }
    }
}
