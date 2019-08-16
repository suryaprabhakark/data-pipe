package com.github.datapipe.sources.mysql.models;

import java.time.LocalDateTime;

public class BinLogSyncStatus {
    private String pipeId;
    private LocalDateTime updateAt;
    private String binLogFile;
    private long logPos;
    private Long gtid;

    public String getPipeId() {
        return pipeId;
    }

    public void setPipeId(String pipeId) {
        this.pipeId = pipeId;
    }

    public LocalDateTime getUpdateAt() {
        return updateAt;
    }

    public void setUpdateAt(LocalDateTime updateAt) {
        this.updateAt = updateAt;
    }

    public String getBinLogFile() {
        return binLogFile;
    }

    public void setBinLogFile(String binLogFile) {
        this.binLogFile = binLogFile;
    }

    public long getLogPos() {
        return logPos;
    }

    public void setLogPos(long logPos) {
        this.logPos = logPos;
    }

    public Long getGtid() {
        return gtid;
    }

    public void setGtid(Long gtid) {
        this.gtid = gtid;
    }

    public static String ddl(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + tableName +
                "  ( " +
                "     pipe_id    VARCHAR(100) NOT NULL," +
                "     binlogfile VARCHAR(512) NOT NULL," +
                "     log_pos    BIGINT(20)," +
                "     gtid       BIGINT(20)," +
                "     updated_at TIMESTAMP" +
                "  )" +
                "engine = innodb ";
    }


}
