//package com.github.datapipe.common.db;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Objects;
//
//public class Table {
//    private String name;
//    private List<Column> columns;
//
//    public String getName() {
//        return name;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public List<Column> getColumns() {
//        return columns;
//    }
//
//    public void setColumns(List<Column> columns) {
//        this.columns = columns;
//    }
//
//    public void addColumn(Column column) {
//        if (Objects.isNull(columns)) columns = new ArrayList<>();
//        columns.add(column);
//    }
//}
