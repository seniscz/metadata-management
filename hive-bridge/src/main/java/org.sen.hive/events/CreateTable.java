package org.sen.hive.events;


import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;

import java.util.Collections;
import java.util.Map;

/**
 * @author
 */
public class CreateTable extends BaseHiveEvent {
    private final boolean skipTempTables;

    public CreateTable(HiveHookContext context, boolean skipTempTables) {
        super(context);

        this.skipTempTables = skipTempTables;
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws Exception {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_TABLE);
        Table table = null;
        Entity currEntity = null;

        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                currEntity = entity;
                table = entity.getTable();

                if (table != null) {
                    table = getHive().getTable(table.getDbName(), table.getTableName());

                    if (table != null) {
                        // If its an external table, even though the temp table skip flag is on, 
                        // we create the table since we need the HDFS path to temp table lineage.
                        if (skipTempTables && table.isTemporary() && !TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                            table = null;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        if (table != null) {
            Map<String, Object> tblEntity = toTableEntity(table);
            // add table partition
            if (currEntity != null) {
                addTablePartition(tblEntity, currEntity.getPartition());
            }
            ret.setAttribute(HIVE_TYPE_TABLE, tblEntity);

            if (isHBaseStore(table)) {
                // This create lineage to HBase table in case of Hive on HBase
                Map<String, Object> hbaseTableEntity = toReferencedHBaseTable(table);

                if (hbaseTableEntity != null) {
                    if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                        ret.setAttribute(ATTRIBUTE_INPUTS, Collections.singletonList(hbaseTableEntity));
                    } else {
                        ret.setAttribute(ATTRIBUTE_OUTPUTS, Collections.singletonList(hbaseTableEntity));
                    }
                }
            } else {
                if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                    Map<String, Object> hdfsPathEntity = getHDFSPathEntity(table.getDataLocation());
                    ret.setAttribute(ATTRIBUTE_INPUTS, Collections.singletonList(hdfsPathEntity));
                }
            }
        }
        return ret;
    }
}
