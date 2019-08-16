package com.github.datapipe.sources.mysql;

import com.github.datapipe.common.Source;
import com.github.datapipe.common.events.EventsBatch;
import com.github.datapipe.common.metadata.MysqlMetadataStore;
import com.github.datapipe.sources.mysql.connectors.MySqlMetadataConnector;
import com.github.datapipe.sources.mysql.connectors.MysqlBinLogConnector;
import com.github.datapipe.sources.mysql.connectors.MysqlQueryConnector;
import com.github.datapipe.sources.mysql.models.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Sql2o;

import java.io.IOException;
import java.util.*;

public class MysqlSource implements Source {
    private static Logger logger = LoggerFactory.getLogger(MysqlSource.class);

    private Config config;
    private MysqlSourceConfig mysqlSourceConfig;
    private MysqlQueryConnector queryConnector;
    private MysqlBinLogConnector binLogConnector;
    // used for storing pipe metadata
    private MySqlMetadataConnector metadataConnector;
    private String readerMode;
    private Map<String, Table> tables;

    public MysqlSource(Config config) {
        this.config = config;
        this.mysqlSourceConfig = MysqlSourceConfig.from(config.getConfig("mysql"));
        metadataConnector = new MySqlMetadataConnector(config.getConfig("metadata-store"));
    }

    public MysqlSource() {
        this.config = ConfigFactory.load().getConfig("source.mysql");
    }

    public void init() throws IOException {
        logger.info("Starting query connector...");
        this.queryConnector = new MysqlQueryConnector(this.mysqlSourceConfig);

        logger.info("fetching tables metadata...");
        List<TableColumn> allTableColumns = this.queryConnector.getTablesMetadata();
        tables = getTables(allTableColumns);

        logger.info("Found {} tables..", tables.size());
        tables.forEach((name, table) -> {
            logger.info("Found table {} and fetched schema with {} columns", name, table.getColumns().size());
        });

        logger.info("Creating tables required to store metadata");
        createMetadataTablesIfNotExist();
        logger.info("Filtering tables by configuration");
        Map<String, Table> tablesToSync = new HashMap<>();
        Optional.ofNullable(this.mysqlSourceConfig.getTablesToSync()).ifPresent(tableList -> {
            if (tableList.contains("all")) {
                tablesToSync.putAll(tables);
            } else {
                tableList.forEach(t -> {
                    Optional.ofNullable(tables.get(t)).ifPresent(table -> tablesToSync.put(t, table));
                });
            }
        });

        // tables to sync by filtering excluded
        if (tablesToSync.isEmpty()) {
            Optional.ofNullable(this.mysqlSourceConfig.getTablesToExclude()).ifPresent(tableList -> {
                tables.forEach((name, table) -> {
                    if (!tableList.contains(name)) tablesToSync.put(name, table);
                });
            });
        }

        // Assume all tables to be synced if non of the configuration present
        if (tablesToSync.isEmpty()) {
            tablesToSync.putAll(tables);
        }

//        this.binLogConnector = new MysqlBinLogConnector(this.mysqlSourceConfig);
//        this.binLogConnector.startReplication();
//        this.queryConnector.getTablesMetadata();
    }

    private Map<String, Table> getTables(List<TableColumn> tableColumns) {
        Map<String, Table> tables = new LinkedHashMap<>();
        tableColumns.forEach(column -> {
            Table table = tables.computeIfAbsent(column.getTableName(), (key) -> {
                Table t = new Table();
                t.setName(key);
                return t;
            });
            table.addColumn(getColumn(column));
        });
        return tables;
    }

    private Column getColumn(TableColumn tc) {
        Column column = new Column();
        column.setName(tc.getColumnName());
        column.setDataType(tc.getDataType());
        column.setPrimary(tc.isPrimary());
        return column;
    }

    /**
     * Commits the batch for process restart
     *
     * @param batch
     */
    public void commit(EventsBatch batch) {
        // Commit the batch
    }

    /**
     * Max Batch size should be set
     *
     * @return
     */
    public EventsBatch readEvents() {
        // Read events
        return null;
    }






    private String tableMetadataStoreTable = "mysql_table_sync_status";
    private String binLogMetadataStoreTable = "mysql_binlog_sync_status";

    private void createMetadataTablesIfNotExist() {
        logger.info("Creating table {} if not exists...", tableMetadataStoreTable);
        metadataConnector.createTable(TableSyncStatus.ddl(tableMetadataStoreTable));
        logger.info("Creating table {} if not exists...", binLogMetadataStoreTable);
        metadataConnector.createTable(BinLogSyncStatus.ddl(binLogMetadataStoreTable));
        logger.info("Completed creating necessary tables for mysql source");
    }
}
