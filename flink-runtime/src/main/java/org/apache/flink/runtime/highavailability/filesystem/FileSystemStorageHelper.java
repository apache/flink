/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.highavailability.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link FileSystemStorageHelper} implementation which stores the state in the given filesystem path.
 *
 * @param <T> The type of the data that can be stored by this storage helper.
 */
public class FileSystemStorageHelper<T extends Serializable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemStorageHelper.class);


    private final Path rootPath;

    private final String prefix;

    private final FileSystem fs;

    /**
     * Storage constructor.
     *
     * 	@param rootPath		    Root path (string)
     * 	@param prefix    		prefix
     */

    public FileSystemStorageHelper(String rootPath, String prefix) throws IOException {
        this(new Path(rootPath), prefix);
    }

    /**
     * Storage constructor.
     *
     * 	@param rootPath		    Root path (Path)
     * 	@param prefix    		prefix
     */

    public FileSystemStorageHelper(Path rootPath, String prefix) throws IOException {
        this.rootPath = Preconditions.checkNotNull(rootPath, "Root path");
        this.prefix = Preconditions.checkNotNull(prefix, "Prefix");

        fs = FileSystem.get(rootPath.toUri());
    }

    /**
     * Store data.
     *
     * 	@param state		                    Data to Store
     * 	@param name    	                        Name of the file to store in
     * 	@return handle to the file where data is stored
     */

    public File store(T state, String name) throws Exception {
        return store(state, new Path(rootPath.getPath() + "/" + prefix + "/" + name));
    }

    /**
     * Store data.
     *
     * 	@param state		                    Data to Store
     * 	@param name    	                        Name of the file to store in
     * 	@param JobID   	                        Job ID
     * 	@return handle to the file where data is stored
     */

    public File store(T state, String JobID, String name) throws Exception {
        return store(state, new Path(rootPath.getPath() + "/" + prefix + "/" + JobID + "/" + name));
    }

    /**
     * Store data.
     *
     * 	@param state		                    Data to Store
     * 	@param filePath   	                    Pathe to the file to store in
     * 	@return handle to the file where data is stored
     */

    private File store(T state, Path filePath) throws Exception {

        try (FSDataOutputStream outStream = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE)) {
            InstantiationUtil.serializeObject(outStream, state);
            return new File(filePath.getPath());
        }
        catch (Exception e) {
            LOG.info("Could not open output stream for state backend" );
            throw new Exception("Could not open output stream for state backend", e);
        }
    }

    /**
     * Clean up directory.
     */

    public void cleanup(){
        File folder = new File(rootPath + "/" + prefix);
        cleanup(folder);
    }

    /**
     * Clean up directory.
     * 	@param folder		                    Starting folder
     */
    private void cleanup(File folder){
        List<File> files = getChildren(folder, false);
        for (File f : files) {
            if (f.isDirectory()) {
                cleanup(f);
            }
            if (f.exists()) {
                f.delete();
            }
        }
    }

    /**
     * Remove job id and all related files.
     * 	@param jobID		                    job id
     */
    public void removeJobId(String jobID){
        File folder = new File(rootPath + "/" + prefix + "/" + jobID);
        cleanup(folder);
    }

    /**
     * Get list of files for job ID.
     * 	@param jobID		                    job
     *  @return  LIst of file handles
     */
    public List<File> getFilesPerJob(String jobID) {
        File folder = new File(rootPath + "/" + prefix + "/" + jobID);
        return getChildren(folder, true);
    }

    /**
     * Get list of files
     *  @return  LIst of file handles
     */
    public List<File> getFiles() {
        File folder = new File(rootPath + "/" + prefix);
        return getChildren(folder, true);
    }

    /**
     * Get list of job ids
     *  @return  LIst of file handles
     */

    public List<String> getJobIDs() {
        List<File> jobs = getFiles();
        List<String> ids = new LinkedList<>();
        for (File f : jobs) {
            ids.add(f.getName());
        }
        return ids;
    }

    /**
     * Get input stream for file
     * 	@param file		                    File
     *  @return  Data stream
     */

    public FSDataInputStream getInputStream(File file) throws Exception {
        return fs.open(new Path(file.getAbsolutePath()));
    }

    /**
     * Get a list of file children
     * 	@param file		                        File (might be null or do not exist)
     * 	@param skip		                        skip hidden files
     *  @return  Data stream
     */

    private List<File> getChildren(File file, boolean skip){
        if((file != null) && (file.exists()) && (file.isDirectory())) {
            return filesToList(file.listFiles(), skip);
        }
        LOG.info("No childrens found for file " + file.getPath());
        return new LinkedList<>();
    }

    /**
     * Convert an array of files to to list
     * 	@param files		                    Array of files (might be null)
     * 	@param skip		                        skip hidden files
     *  @return  Data stream
     */

    private List<File> filesToList(File[] files, boolean skip){
        List<File> list = new LinkedList<>();
        if((files != null)) {
            for(File f : files){
                if(skip) {
                    if (!f.getName().startsWith(".")) {
                        list.add(f);
                    }
                }
                else {
                    list.add(f);
                }
            }
        }
        return list;
    }
}