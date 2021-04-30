package org.apache.flink.python.util;

import org.apache.flink.util.OperatingSystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

public class DecompressUtils {
    public static boolean isUnix() {
        return OperatingSystem.isLinux()
                || OperatingSystem.isMac()
                || OperatingSystem.isFreeBSD()
                || OperatingSystem.isSolaris();
    }

    public static void addIfBitSet(
            int mode,
            int pos,
            Set<PosixFilePermission> posixFilePermissions,
            PosixFilePermission posixFilePermissionToAdd) {
        if ((mode & 1L << pos) != 0L) {
            posixFilePermissions.add(posixFilePermissionToAdd);
        }
    }

    public static void setFilePermission(File file, int mode) throws IOException {
        Path path = Paths.get(file.toURI());
        Set<PosixFilePermission> permissions = new HashSet<>();
        DecompressUtils.addIfBitSet(mode, 8, permissions, PosixFilePermission.OWNER_READ);
        DecompressUtils.addIfBitSet(mode, 7, permissions, PosixFilePermission.OWNER_WRITE);
        DecompressUtils.addIfBitSet(mode, 6, permissions, PosixFilePermission.OWNER_EXECUTE);
        DecompressUtils.addIfBitSet(mode, 5, permissions, PosixFilePermission.GROUP_READ);
        DecompressUtils.addIfBitSet(mode, 4, permissions, PosixFilePermission.GROUP_WRITE);
        DecompressUtils.addIfBitSet(mode, 3, permissions, PosixFilePermission.GROUP_EXECUTE);
        DecompressUtils.addIfBitSet(mode, 2, permissions, PosixFilePermission.OTHERS_READ);
        DecompressUtils.addIfBitSet(mode, 1, permissions, PosixFilePermission.OTHERS_WRITE);
        DecompressUtils.addIfBitSet(mode, 0, permissions, PosixFilePermission.OTHERS_EXECUTE);
        Files.setPosixFilePermissions(path, permissions);
    }
}
