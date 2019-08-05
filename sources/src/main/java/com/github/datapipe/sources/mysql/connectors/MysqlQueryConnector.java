package com.github.datapipe.sources.mysql.connectors;

import com.github.datapipe.sources.mysql.MysqlSourceConfig;
import com.github.datapipe.sources.mysql.models.TableColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MysqlQueryConnector implements MysqlConnector {
    private static Logger logger = LoggerFactory.getLogger(MysqlQueryConnector.class);
    private Sql2o connector;
    private MysqlSourceConfig config;

    public MysqlQueryConnector(MysqlSourceConfig config) {
        connector = new Sql2o(config.getUrl(), config.getUsername(), config.getPassword());
        this.config = config;

    }

    public List<TableColumn> getTablesMetadata() {
        return select("SELECT " + String.join(",", TableColumn.sqlMappings().keySet()) + " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :tableSchema",
                Collections.singletonMap("tableSchema", config.getSchema()),
                TableColumn.class,
                TableColumn.sqlMappings()
        );
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
