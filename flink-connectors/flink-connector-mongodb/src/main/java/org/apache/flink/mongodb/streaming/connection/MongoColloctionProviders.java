package org.apache.flink.mongodb.streaming.connection;

import org.apache.flink.util.Preconditions;

import com.mongodb.WriteConcern;

public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String database;

        private String collection;

        private String servers;

        private String userName;

        private String password;

        private WriteConcern writeConcern;

        private boolean retryWrites;

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

        public Builder setWriteConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        public Builder setRetryWrites(boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        public Builder setTimeout(long timeout) {
            this.timeout = timeout;
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
            return new MongoSingleCollectionProvider(servers, userName, password, writeConcern, retryWrites, timeout, database, collection);
        }
    }
}
