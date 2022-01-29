package org.apache.flink.connector.mongodb.util;

import org.apache.flink.connector.mongodb.config.MongoConfigConstants;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Properties;

/** Tests for {@link MongoGeneralUtil}. */
public class MongoGeneralUtilTest {

    @Test
    public void testGetMongoCredentialPlain() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "PLAIN");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_SOURCE, "source");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_PASSWORD, "password");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.PLAIN);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
        Assertions.assertThat(credentials.getSource()).isEqualTo("source");
        Assertions.assertThat(credentials.getPassword()).isEqualTo("password".toCharArray());
    }

    @Test
    public void testGetMongoCredentialGssApi() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "GSSAPI");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.GSSAPI);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
    }

    @Test
    public void testGetMongoCredentialScramSha1() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "SCRAM_SHA_1");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_SOURCE, "source");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_PASSWORD, "password");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.SCRAM_SHA_1);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
        Assertions.assertThat(credentials.getSource()).isEqualTo("source");
        Assertions.assertThat(credentials.getPassword()).isEqualTo("password".toCharArray());
    }

    @Test
    public void testGetMongoCredentialScramSha256() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "SCRAM_SHA_256");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_SOURCE, "source");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_PASSWORD, "password");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.SCRAM_SHA_256);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
        Assertions.assertThat(credentials.getSource()).isEqualTo("source");
        Assertions.assertThat(credentials.getPassword()).isEqualTo("password".toCharArray());
    }

    @Test
    public void testGetMongoCredentialAws() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "MONGODB_AWS");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_PASSWORD, "password");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.MONGODB_AWS);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
        Assertions.assertThat(credentials.getPassword()).isEqualTo("password".toCharArray());
    }

    @Test
    public void testGetMongoCredentialX509() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "MONGODB_X509");
        properties.setProperty(MongoConfigConstants.MONGO_CREDENTIAL_USERNAME, "username");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.MONGODB_X509);
        Assertions.assertThat(credentials.getUserName()).isEqualTo("username");
    }

    @Test
    public void testGetMongoCredentialX509WithNullUsername() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_CREDENTIAL_MECHANISM, "MONGODB_X509");

        MongoCredential credentials = MongoGeneralUtil.getMongoCredential(properties);

        Assertions.assertThat(credentials.getAuthenticationMechanism())
                .isEqualTo(AuthenticationMechanism.MONGODB_X509);
        Assertions.assertThat(credentials.getUserName()).isNull();
    }

    @Test
    public void testServerApi() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_SERVER_API_VERSION, "V1");
        ServerApi serverApi = MongoGeneralUtil.getServerApi(properties);
        Assertions.assertThat(serverApi.getVersion()).isEqualTo(ServerApiVersion.V1);
        Assertions.assertThat(serverApi.getStrict()).isEmpty();
        Assertions.assertThat(serverApi.getDeprecationErrors()).isEmpty();
    }

    @Test
    public void testServerApiWithDeprecationErrorsSetToTrue() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_SERVER_API_VERSION, "V1");
        properties.put(MongoConfigConstants.MONGO_SERVER_API_DEPRECATION_ERRORS, "true");
        ServerApi serverApi = MongoGeneralUtil.getServerApi(properties);
        Assertions.assertThat(serverApi.getVersion()).isEqualTo(ServerApiVersion.V1);
        Assertions.assertThat(serverApi.getDeprecationErrors()).isPresent().get().isEqualTo(true);
    }

    @Test
    public void testServerApiWithStrictSetToTrue() {
        Properties properties = new Properties();
        properties.put(MongoConfigConstants.MONGO_SERVER_API_VERSION, "V1");
        properties.put(MongoConfigConstants.MONGO_SERVER_API_STRICT, "true");
        ServerApi serverApi = MongoGeneralUtil.getServerApi(properties);
        Assertions.assertThat(serverApi.getVersion()).isEqualTo(ServerApiVersion.V1);
        Assertions.assertThat(serverApi.getStrict()).isPresent().get().isEqualTo(true);
    }

    @Test
    public void testMongoClientSettingsConnectionString() {
        Properties properties = new Properties();
        properties.put(
                MongoConfigConstants.CONNECTION_STRING,
                "mongodb://host1:1234/mydb?replicaSet=replicaSetName&retryWrites=true");
        MongoClientSettings mongoClientSettings =
                MongoGeneralUtil.getMongoClientSettings(properties);

        Assertions.assertThat(mongoClientSettings.getClusterSettings().getRequiredReplicaSetName())
                .isEqualTo("replicaSetName");
        Assertions.assertThat(mongoClientSettings.getRetryWrites()).isTrue();
        Assertions.assertThat(mongoClientSettings.getClusterSettings().getHosts())
                .hasSize(1)
                .first()
                .extracting(ServerAddress::getHost)
                .isEqualTo("host1");
    }
}
