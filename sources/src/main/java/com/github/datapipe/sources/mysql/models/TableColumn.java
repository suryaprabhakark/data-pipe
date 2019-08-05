package com.github.datapipe.sources.mysql.models;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * To fetch tables and columns data from schema
 * <p>
 * * Query:
 * * select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
 * * DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
 * * CHARACTER_SET_NAME, COLLATION_NAME from INFORMATION_SCHEMA.COLUMNS;
 * *
 */
public class TableColumn {
    private String tableName;
    private String columnName;
    private int ordinalPosition;
    private String columnDefault;
    private String columnKey;
    private Boolean isNullable;
    private String dataType;
    private String charMaxLength;
    private Integer numericPrecision;
    private Integer numericScale;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public void setColumnDefault(String columnDefault) {
        this.columnDefault = columnDefault;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(String columnKey) {
        this.columnKey = columnKey;
    }

    public Boolean getNullable() {
        return isNullable;
    }

    public void setNullable(Boolean nullable) {
        isNullable = nullable;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getCharMaxLength() {
        return charMaxLength;
    }

    public void setCharMaxLength(String charMaxLength) {
        this.charMaxLength = charMaxLength;
    }

    public Integer getNumericPrecision() {
        return numericPrecision;
    }

    public void setNumericPrecision(Integer numericPrecision) {
        this.numericPrecision = numericPrecision;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public void setNumericScale(Integer numericScale) {
        this.numericScale = numericScale;
    }

    public boolean isPrimary() {
        return Optional.ofNullable(this.columnKey).map($ -> $.equalsIgnoreCase("pri")).orElse(false);
    }

    /**
     * for sql2o mappings
     *
     * @return
     */
    public static Map<String, String> sqlMappings() {
        Map<String, String> mappings = new LinkedHashMap<>();
        mappings.put("TABLE_NAME", "tableName");
        mappings.put("COLUMN_NAME", "columnName");
        mappings.put("ORDINAL_POSITION", "ordinalPosition");
        mappings.put("COLUMN_DEFAULT", "columnDefault");
        mappings.put("IS_NULLABLE", "isNullable");
        mappings.put("DATA_TYPE", "dataType");
        mappings.put("CHARACTER_MAXIMUM_LENGTH", "charMaxLength");
        mappings.put("NUMERIC_PRECISION", "numericPrecision");
        mappings.put("NUMERIC_SCALE", "numericScale");
        mappings.put("COLUMN_KEY", "columnKey");
        return mappings;
    }
}
