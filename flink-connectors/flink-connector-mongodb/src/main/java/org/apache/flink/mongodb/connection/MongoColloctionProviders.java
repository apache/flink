package org.apache.flink.mongodb.connection;

import org.apache.flink.util.Preconditions;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

import java.util.List;

public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String servers;

        private String userName;

        private String password;

        private String database;

        private String collection;

        private WriteConcern writeConcern;

        private boolean retryWrites;

        private ReadConcern readConcern;

        private boolean retryReads;

        private ReadPreference readPreference;

        private List<MongoCompressor> compressorList;

        private long timeout;

        public Builder setdatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder setServers(String servers) {
            this.servers = servers;
            return this;
        }

        public Builder setUserName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param writeConcern the write concern
         *
         * @return this
         *
         * @see MongoClientSettings#getWriteConcern()
         */
        public Builder setWriteConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        /**
         * Sets whether writes should be retried if they fail due to a network error.
         *
         * <p>Starting with the 3.11.0 release, the default value is true</p>
         *
         * @param retryWrites sets if writes should be retried if they fail due to a network error.
         *
         * @return this
         *
         * @mongodb.server.release 3.6
         */
        public Builder setRetryWrites(boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        /**
         * The timeout selected by the server (in milliseconds), specifying the time when the driver failed to select a server from the cluster and threw an exception.
         * You can set the value of this parameter as needed, such as waiting patiently or returning errors quickly. The default is 30,000 (30 seconds), which is enough time to elect a new master node in the classic fail-over phase.
         * 0 indicates that no server is available and will timeout immediately.
         * Negative values mean waiting indefinitely.
         *
         * @param timeout
         *
         * @return this
         */
        public Builder setTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets whether reads should be retried if they fail due to a network error.
         *
         * @param retryReads sets if reads should be retried if they fail due to a network error.
         *
         * @return this
         *
         * @mongodb.server.release 3.6
         * @since 3.11
         */
        public Builder setRetryReads(boolean retryReads) {
            this.retryReads = retryReads;
            return this;
        }

        /**
         * The read policy (isolation level) determines how data is returned in the cluster environment——
         * Local – read directly from the local, regardless of whether the data has been synchronized in other nodes of the cluster.
         * The default value is. Majority – only data successfully written to most nodes is read.
         * Linearizable – server version: introduced in version 3.4. It is similar to majority, but it fixes some bugs in majority, but it consumes more performance than majority.
         * Linearizable is linear for concurrent read and write operations of the same document. Snapshot – server version: introduced in version 4.0.
         * Mongodb supports replica set multi document transactions from 4.0, and provides snapshot isolation level.
         * At the beginning of the transaction, a wiredtiger snapshot (snapshot) is created to save the status of all transactions of the whole engine at that time, determine which transactions are currently visible and which are not, and then use this snapshot to provide transaction read during the whole transaction process.
         * This option can be used to solve the problem of "dirty reading". For example, a piece of data is read from the primary node, but it is not synchronized to most nodes. Then the primary node fails. After recovery, the primary node will roll back the data that is not synchronized to most nodes, resulting in the previously read data becoming "dirty data".
         * Setting the level of readconcern to majority ensures that the read data has been written to most nodes, and these data will not be rolled back, thus avoiding the problem of "dirty reading".
         * It should be noted that this level can ensure that the read data will not be rolled back, but it does not guarantee that the read data is up-to-date.
         * This option can be used in conjunction with readpreference.
         *
         * @param readConcern the read concern
         *
         * @return this
         *
         * @mongodb.server.release 3.2
         * @mongodb.driver.manual reference/readConcern/ Read Concern
         */
        public Builder setReadConcern(ReadConcern readConcern) {
            this.readConcern = readConcern;
            return this;
        }

        /**
         * List of compressors used to send and receive messages to the mongodb server.
         * The driver will use the first compressor in the list supported by the server configuration.
         * The default is an empty list.
         *
         * @param compressorList
         *
         * @return this
         *
         * @mongodb.server.release 3.4
         */
        public Builder setCompressorList(List<MongoCompressor> compressorList) {
            this.compressorList = compressorList;
            return this;
        }

        /**
         * @param readPreference
         *
         * @return this
         */
        public Builder setReadPreference(ReadPreference readPreference) {
            this.readPreference = readPreference;
            return this;
        }


        public MongoClientProvider build() {
            Preconditions.checkNotNull(servers, "Servers must not be null");
            Preconditions.checkNotNull(userName, "UserName must not be null");
            Preconditions.checkNotNull(password, "Password must not be null");
            Preconditions.checkNotNull(writeConcern, "WriteConcern must not be null");
            Preconditions.checkNotNull(retryWrites, "RetryWrites must not be null");
            Preconditions.checkNotNull(timeout, "Timeout must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            //private ReadConcern readConcern;
            //
            //        private boolean retryReads;
            //
            //        private ReadPreference readPreference;
            //
            //        private List<MongoCompressor> compressorList;
            return new MongoSingleCollectionProvider(
                    servers,
                    userName,
                    password,
                    writeConcern,
                    retryWrites,
                    timeout,
                    database,
                    collection);
        }
    }
}
