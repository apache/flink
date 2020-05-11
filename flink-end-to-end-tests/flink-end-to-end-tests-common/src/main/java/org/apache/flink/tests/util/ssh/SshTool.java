/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util.ssh;

import com.hierynomus.sshj.signature.SignatureEdDSA;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.signature.Signature;
import net.schmizz.sshj.signature.SignatureDSA;
import net.schmizz.sshj.signature.SignatureECDSA;
import net.schmizz.sshj.signature.SignatureRSA;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * SSH test utilities.
 */

public class SshTool {

	private String username;
	private int port;
	private String authorizedFile;
	private String knownHosts;
	public Config config = new DefaultConfig();
	public SSHClient ssh = null;

	public void setConfig() {
		List<Factory.Named<Signature>> signatureFactories = new ArrayList<Factory.Named<Signature>>();
		signatureFactories.add(new SignatureRSA.Factory());
		signatureFactories.add(new SignatureEdDSA.Factory());
		signatureFactories.add(new SignatureECDSA.Factory256());
		signatureFactories.add(new SignatureECDSA.Factory384());
		signatureFactories.add(new SignatureECDSA.Factory521());
		signatureFactories.add(new SignatureDSA.Factory());

		this.config.setSignatureFactories(signatureFactories);
	}

	public SshTool() {
		this.username = System.getProperty("user.name");
		this.port = 22;
		this.authorizedFile = System.getProperty("user.home") + "/.ssh/id_rsa";
		this.knownHosts = System.getProperty("user.home") + "/.ssh/known_hosts";
		setConfig();
	}

	public void connect(String host) throws IOException {
		try {
			this.ssh = new SSHClient(this.config);
			this.ssh.loadKnownHosts();
			this.ssh.addHostKeyVerifier(new PromiscuousVerifier());
			this.ssh.connect(host, this.port);
			this.ssh.authPublickey(this.username, this.authorizedFile);
		} catch (Exception e) {
			System.out.println("login failed!");
			e.printStackTrace();
			throw e;
		}
	}

	public void copyDirectoryTo(Path localDirectory, Path remoteDirectory, String host) throws IOException {
		this.connect(host);
		if (this.ssh != null) {
			ssh.newSCPFileTransfer()
				.upload(localDirectory.toAbsolutePath().toString(), remoteDirectory.toAbsolutePath().toString());
			ssh.close();
		}
	}

	public void copyDirectoryFrom(Path remoteDirectory, Path localDirectory, String host) throws IOException {
		this.connect(host);
		if (this.ssh != null) {
			this.ssh.newSCPFileTransfer().download(remoteDirectory.toAbsolutePath().toString(),
				localDirectory.toAbsolutePath().toString());
			this.ssh.close();
		}
	}

	public void copyDirectoryFrom1(Path remoteDirectory, Path localDirectory, String host) throws IOException {
		this.connect(host);
		if (this.ssh != null) {
			ssh.newSCPFileTransfer()
				.download(remoteDirectory.toAbsolutePath().toString(), localDirectory.toAbsolutePath().toString());
			this.ssh.close();
		}
	}

	public void runBlocking(String host, String... commands) throws IOException {
		this.connect(host);
		if (this.ssh != null) {
			try (Session session = ssh.startSession()) {
				session.exec(String.join(" ", commands)).join();
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.ssh.close();
		}
	}

	public static SshTool fromSystemProperties(){
		return new SshTool();
	}
}
