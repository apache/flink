package org.apache.flink.tools.ci.licensecheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

/** Utility for checking all things related to License and Notice files. */
public class LicenseChecker {
    // ---------------------------------------- Launcher ---------------------------------------- //

    private static final Logger LOG = LoggerFactory.getLogger(LicenseChecker.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println(
                    "Usage: LicenseChecker <pathMavenBuildOutput> <pathFlinkRoot> <pathFlinkDeployed>");
            System.exit(1);
        }
        LOG.warn(
                "THIS UTILITY IS ONLY CHECKING FOR COMMON LICENSING MISTAKES. A MANUAL CHECK OF THE NOTICE FILES, DEPLOYED ARTIFACTS, ETC. IS STILL NEEDED!");

        int severeIssueCount = NoticeFileChecker.run(new File(args[0]), Paths.get(args[1]));

        severeIssueCount += JarFileChecker.checkPath(Paths.get(args[2]));

        if (severeIssueCount > 0) {
            LOG.warn("Found a total of {} severe license issues", severeIssueCount);

            System.exit(1);
        }
        LOG.info("License check completed without severe issues.");
    }
}
