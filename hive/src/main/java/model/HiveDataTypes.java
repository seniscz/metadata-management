package model;

/**
 * @ClassName HiveDataTypes
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/21 11:33
 * @Version 1.0
 **/
public enum  HiveDataTypes {
    // Enums
    HIVE_OBJECT_TYPE,
    HIVE_PRINCIPAL_TYPE,
    HIVE_RESOURCE_TYPE,

    // Structs
    HIVE_SERDE,
    HIVE_ORDER,
    HIVE_RESOURCEURI,

    // Classes
    HIVE_DB,
    HIVE_STORAGEDESC,
    HIVE_TABLE,
    HIVE_COLUMN,
    HIVE_PARTITION,
    HIVE_INDEX,
    HIVE_ROLE,
    HIVE_TYPE,
    HIVE_PROCESS,
    HIVE_COLUMN_LINEAGE,
    HIVE_PROCESS_EXECUTION,
    // HIVE_VIEW,
    ;

    public String getName() {
        return name().toLowerCase();
    }


}
