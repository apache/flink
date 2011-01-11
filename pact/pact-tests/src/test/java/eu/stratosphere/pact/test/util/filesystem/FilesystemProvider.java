package eu.stratosphere.pact.test.util.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FilesystemProvider {

	public void start() throws Exception;
	
	public void stop();
	
	public boolean createFile(String fileName, String fileContent) throws IOException;

	public boolean copyFile(String localFile, String hdfsFile) throws IOException;
	
	public boolean createDir(String dirName) throws IOException;

	public boolean delete(String path, boolean recursive) throws IOException;
	
	public OutputStream getOutputStream(String file) throws IOException;

	public InputStream getInputStream(String file) throws IOException;
	
	public String getTempDirPath();
	
	public String[] listFiles(String dir) throws IOException;
	
	public boolean isDir(String file) throws IOException;
	
	public String getURIPrefix();
	
}
