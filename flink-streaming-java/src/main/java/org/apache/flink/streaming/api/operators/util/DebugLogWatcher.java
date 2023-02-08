package org.apache.flink.streaming.api.operators.util;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugLogWatcher{
    private static final Logger LOG = LoggerFactory.getLogger(DebugLogWatcher.class);

    private Timer timer;
    private DebugLogConfigReader configReader;
    private Random rand;
    public DebugLogWatcher(String fileName, String operatorName, int seconds) {
        LOG.info("Initializing debug log watcher for " + fileName);
        timer = new Timer();
        configReader = new DebugLogConfigReader(fileName, operatorName);
        timer.schedule(configReader, 0, seconds*1000);
        rand = new Random();
    }

    class DebugLogConfigReader extends TimerTask {
        private String fileName;
        private String operatorName;
        private int logFrequency;
        DebugLogConfigReader(String fileName, String operatorName) {
            this.fileName = fileName;
            this.operatorName = operatorName;
            this.logFrequency = -1;
        }

        public void run() {
            File f = new File(this.fileName);
            int newLogFrequency = -1;
            if (f.exists()) {
                try {
                    FileReader fr = new FileReader(f);
                    BufferedReader reader = new BufferedReader(fr);
                    String line = reader.readLine();
                    reader.close();
                    if (line != null) {
                        try {
                            newLogFrequency = Integer.parseInt(line);
                        } catch (NumberFormatException ex){
                            LOG.error("Failed to parse " + line + " from " + this.fileName);
                        }
                    }
                } catch (FileNotFoundException ex) {
                    LOG.error("Couldn't read the file " + this.fileName);
                } catch (IOException ex) {
                    LOG.error("Couldn't read the file " + this.fileName);
                }
            }
            if (newLogFrequency != this.logFrequency) {
                LOG.info("Setting the log frequency to " + newLogFrequency + " for operator " + this.operatorName);
                this.logFrequency = newLogFrequency;
            }
        }

        public int getLogFrequency() {
            return this.logFrequency;
        }
    }


    public void close() {
        this.timer.cancel();
    }

    public boolean shouldLog() {
        int freq =  this.configReader.getLogFrequency();
        if (freq <= 0) {
            return false;
        }
        int x = this.rand.nextInt(freq);
        return x == 0;
    }
}
