package com.github.datapipe.sources.mysql.connectors;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MySqlMetadataConnector {
    private static Logger logger = LoggerFactory.getLogger(MySqlMetadataConnector.class);
    private Sql2o connector;
    private Config config;

    public MySqlMetadataConnector(Config config) {
        connector = new Sql2o(config.getString("url"), config.getString("username"), config.getString("password"));
        this.config = config;
    }

    public void createTable(String query) {
        try (Connection con = connector.open()) {
            Query q = con.createQuery(query);
            q.executeUpdate();
        } catch (Throwable t) {
            logger.error("Error running query {}:", query, t);
            throw t;
        }
    }

    private <T> List<T> select(String query, Map<String, Object> params, Class<T> clazz) {
        return select(query, params, clazz, Collections.emptyMap());
    }

    private <T> List<T> select(String query, Map<String, Object> params, Class<T> clazz, Map<String, String> columnMappings) {
        try (Connection con = connector.open()) {
            Query q = con.createQuery(query);
            columnMappings.forEach(q::addColumnMapping);
            params.forEach(q::addParameter);
            return q.executeAndFetch(clazz);
        } catch (Throwable t) {
            logger.error("Error running query:", t);
            throw t;
        }
    }

    private boolean update(String query, Map<String, Object> params) {
        try (Connection con = connector.open()) {
            Query q = con.createQuery(query);
            params.forEach(q::addParameter);
            return q.executeUpdate().getResult() > 0;
        } catch (Throwable t) {
            logger.error("Error running query:", t);
            throw t;
        }
    }

}
