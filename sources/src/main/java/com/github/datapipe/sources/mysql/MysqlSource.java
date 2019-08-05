package com.github.datapipe.sources.mysql;

import com.github.datapipe.common.Source;
import com.github.datapipe.common.events.EventsBatch;
import com.github.datapipe.sources.mysql.connectors.MysqlBinLogConnector;
import com.github.datapipe.sources.mysql.connectors.MysqlQueryConnector;
import com.github.datapipe.sources.mysql.models.Column;
import com.github.datapipe.sources.mysql.models.Table;
import com.github.datapipe.sources.mysql.models.TableColumn;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MysqlSource implements Source {
    private static Logger logger = LoggerFactory.getLogger(MysqlSource.class);

    private Config config;
    private MysqlSourceConfig mysqlSourceConfig;
    private MysqlQueryConnector queryConnector;
    private MysqlBinLogConnector binLogConnector;
    private String readerMode;
    private Map<String, Table> tables;

    public MysqlSource(Config config) {
        this.config = config;
        this.mysqlSourceConfig = MysqlSourceConfig.from(config);
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

    // table information to store
    // {'last_pk_fetched', 'max_pk_values', 'version', 'initial_full_table_complete'}


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
}
