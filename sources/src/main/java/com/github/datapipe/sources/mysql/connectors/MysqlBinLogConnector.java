package com.github.datapipe.sources.mysql.connectors;

import com.github.datapipe.sources.mysql.MysqlSourceConfig;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MysqlBinLogConnector implements MysqlConnector {
    private static final Logger logger = LoggerFactory.getLogger(MysqlBinLogConnector.class);
    private BinaryLogClient binaryLogClient;
    private CustomEventListener customEventListener;


    public MysqlBinLogConnector(MysqlSourceConfig config) {
        this.binaryLogClient = new BinaryLogClient(config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        binaryLogClient.setEventDeserializer(eventDeserializer);
        this.customEventListener = new CustomEventListener(10);
        binaryLogClient.registerEventListener(this.customEventListener);
    }

    public FutureTask<Void> startReplication() throws IOException {
        FutureTask<Void> futureTask = new FutureTask<Void>(() -> {
            binaryLogClient.connect();
            return null;
        });
        futureTask.run();
        return futureTask;
    }

    public void stop() throws IOException {
        binaryLogClient.disconnect();
    }

    public String getBinaryLogName() {
        return binaryLogClient.getBinlogFilename();
    }

    public long getBinaryLogPosition() {
        return binaryLogClient.getBinlogPosition();
    }

    class CustomEventListener implements EventListener {
        private Lock lock = new ReentrantLock();
        private List<Event> events = Collections.synchronizedList(new ArrayList<>());
        private int maxBatchSize;

        public CustomEventListener(int batchSize) {
            this.maxBatchSize = batchSize;
        }

        @Override
        public void onEvent(Event event) {
            if (maxBatchSize <= events.size()) lock.lock();
            events.add(event);
        }

        public List<Event> getEvents() {
            if (lock.tryLock()) {
                List<Event> tempEvents = events;
                events = Collections.synchronizedList(new ArrayList<>());
                lock.unlock();
                return tempEvents;
            } else {
                return Collections.emptyList();
            }
        }
    }
}
