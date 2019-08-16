package com.github.datapipe.sources.mysql.models;

import com.github.datapipe.sources.mysql.enums.SyncStatus;

import java.time.LocalDateTime;

public class TableSyncStatus {
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String pipeId;
    private Long lastPkProcessed;
    private Long maxPkValue;
    private SyncStatus status;

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public String getPipeId() {
        return pipeId;
    }

    public void setPipeId(String pipeId) {
        this.pipeId = pipeId;
    }

    public Long getLastPkProcessed() {
        return lastPkProcessed;
    }

    public void setLastPkProcessed(Long lastPkProcessed) {
        this.lastPkProcessed = lastPkProcessed;
    }

    public Long getMaxPkValue() {
        return maxPkValue;
    }

    public void setMaxPkValue(Long maxPkValue) {
        this.maxPkValue = maxPkValue;
    }

    public SyncStatus getStatus() {
        return status;
    }

    public void setStatus(SyncStatus status) {
        this.status = status;
    }

    public static String ddl(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + tableName +
                "  ( " +
                "     pipe_id           VARCHAR(100) NOT NULL," +
                "     last_pk_processed BIGINT(20)," +
                "     max_pk_value      BIGINT(20)," +
                "     status            VARCHAR(20)," +
                "     start_time        TIMESTAMP," +
                "     end_time          TIMESTAMP" +
                "  )" +
                "engine = innodb ";
    }
}
