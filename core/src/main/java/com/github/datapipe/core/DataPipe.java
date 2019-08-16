package com.github.datapipe.core;

import com.github.datapipe.common.store.PipeStore;
import com.github.datapipe.common.Source;
import com.github.datapipe.common.Target;
import com.github.datapipe.common.exceptions.ConfigurationMissingException;
import com.github.datapipe.sources.mysql.MysqlSource;
import com.github.datapipe.sources.mysql.connectors.MysqlQueryConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataPipe {
    private static Logger logger = LoggerFactory.getLogger(DataPipe.class);

    private Source source;
    private PipeStore store;
    private Target target;

    public void start() throws IOException {
        Config config = ConfigFactory.load();
        if (!config.hasPath("source")) {
            throw new ConfigurationMissingException("Source configurations not found");
        }

        if (!config.hasPath("target")) {
            throw new ConfigurationMissingException("Target configurations not found");
        }

        MysqlSource source = new MysqlSource(config.getConfig("source"));
        source.init();

//        source -> store -> target;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public PipeStore getStore() {
        return store;
    }

    public void setStore(PipeStore store) {
        this.store = store;
    }

    public Target getTarget() {
        return target;
    }

    public void setTarget(Target target) {
        this.target = target;
    }


}
