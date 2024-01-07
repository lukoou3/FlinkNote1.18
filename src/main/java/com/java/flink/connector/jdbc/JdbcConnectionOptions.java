package com.java.flink.connector.jdbc;


import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class JdbcConnectionOptions implements Serializable {
    protected final String url;
    protected final String driverName;
    protected final String username;
    protected final String password;

    protected JdbcConnectionOptions(String url, String driverName, String username, String password) {
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");;
        this.driverName = driverName;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String url;
        private String driverName;
        private String username;
        private String password;

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public JdbcConnectionOptions build() {
            return new JdbcConnectionOptions(url, driverName, username, password);
        }
    }
}
