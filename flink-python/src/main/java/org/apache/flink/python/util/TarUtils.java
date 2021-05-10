package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Utils used to extract tar files. */
@Internal
public class TarUtils {
    public static void unTar(String inFilePath, String targetDirPath)
            throws IOException, InterruptedException {
        File targetDir = new File(targetDirPath);
        if (!targetDir.mkdirs()) {
            if (!targetDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + targetDir);
            }
        }
        boolean gzipped = inFilePath.endsWith("gz");
        if (DecompressUtils.isUnix()) {
            unTarUsingTar(inFilePath, targetDirPath, gzipped);
        } else {
            unTarUsingJava(inFilePath, targetDirPath, gzipped);
        }
    }

    // Copy and simplify from hadoop-common package that is used in YARN
    // See https://github.com/apache/hadoop/blob/7f93349ee74da5f35276b7535781714501ab2457/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileUtil.java
    private static void unTarUsingTar(String inFilePath, String targetDirPath, boolean gzipped)
            throws IOException, InterruptedException {
        inFilePath = makeSecureShellPath(inFilePath);
        targetDirPath = makeSecureShellPath(targetDirPath);
        String untarCommand =
                gzipped
                        ? String.format(
                                "gzip -dc '%s' | (cd '%s' && tar -xf -)", inFilePath, targetDirPath)
                        : String.format("cd '%s' && tar -xf '%s'", targetDirPath, inFilePath);
        Process process = new ProcessBuilder("bash", "-c", untarCommand).start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException(
                    "Error untarring file "
                            + inFilePath
                            + ". Tar process exited with exit code "
                            + exitCode);
        }
    }

    // Follow the pattern suggested in
    // https://commons.apache.org/proper/commons-compress/examples.html
    private static void unTarUsingJava(String inFilePath, String targetDirPath, boolean gzipped)
            throws IOException {
        try (InputStream fi = Files.newInputStream(Paths.get(inFilePath));
                InputStream bi = new BufferedInputStream(fi);
                ArchiveInputStream ai =
                        new TarArchiveInputStream(
                                gzipped ? new GzipCompressorInputStream(bi) : bi)) {
            ArchiveEntry entry;
            while ((entry = ai.getNextEntry()) != null) {
                File f = new File(targetDirPath, entry.getName());
                if (entry.isDirectory()) {
                    if (!f.isDirectory() && !f.mkdirs()) {
                        throw new IOException("failed to create directory " + f);
                    }
                } else {
                    File parent = f.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("failed to create directory " + parent);
                    }
                    OutputStream o = Files.newOutputStream(f.toPath());
                    byte[] buf = new byte[(int) entry.getSize()];
                    IOUtils.readFully(ai, buf, 0, buf.length);
                    IOUtils.copyBytes(new ByteArrayInputStream(buf), o);
                }
            }
        }
    }

    /**
     * Convert a os-native filename to a path that works for the shell
     * and avoids script injection attacks.
     */
    private static String makeSecureShellPath(String filePath) {
        return filePath.replace("'", "'\\''");
    }
}
