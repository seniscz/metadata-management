package org.sen.hive.lineage.bean;

/**
 * column node
 */
public class ColumnNode {
    /**
     * column node id
     */
    private long id;
    /**
     * column name
     */
    private String column;
    /**
     * table id
     */
    private long tableId;
    /**
     * table name
     */
    private String table;
    /**
     * database name
     */
    private String db;

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }
}
