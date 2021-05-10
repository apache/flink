package org.apache.flink.python.util;

import org.apache.flink.util.OperatingSystem;

public class DecompressUtils {
    public static boolean isUnix() {
        return OperatingSystem.isLinux()
                || OperatingSystem.isMac()
                || OperatingSystem.isFreeBSD()
                || OperatingSystem.isSolaris();
    }
}
