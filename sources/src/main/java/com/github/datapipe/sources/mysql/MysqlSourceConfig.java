package com.github.datapipe.sources.mysql;


import com.github.datapipe.common.exceptions.ConfigurationMissingException;
import com.typesafe.config.Config;

import java.util.Optional;

import static com.github.datapipe.common.utils.ConfigUtils.getValue;

public class MysqlSourceConfig {
    private String host;
    private int port;
    private String schema;
    private String url;
    private String username;
    private String password;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public static MysqlSourceConfig from(Config config) {
        Optional<String> host = getValue(config, "host", String.class);
        Optional<Integer> port = getValue(config, "port", Integer.class);
        String schema = config.getString("schema");
        String url = getValue(config, "url", String.class)
                .orElseGet(() -> host.map(h -> "jdbc:mysql://" + h + ":" + port + "/" + schema)
                        .orElseThrow(() -> new ConfigurationMissingException("Neither url nor host config found for mysql.")));

        MysqlSourceConfig sourceConfig = new MysqlSourceConfig();
        host.ifPresent(sourceConfig::setHost);
        port.ifPresent(sourceConfig::setPort);
        sourceConfig.setSchema(schema);
        sourceConfig.setUrl(url);
        sourceConfig.setUsername(config.getString("username"));
        sourceConfig.setPassword(config.getString("password"));
        return sourceConfig;
    }
}
