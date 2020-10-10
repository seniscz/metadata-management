package org.sen.hive.events;

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author
 */
public class DropTable extends BaseHiveEvent {
    public DropTable(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws Exception {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_TABLE);

        List<String> dropTables = null;
        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                String tblQName = getQualifiedName(entity.getTable());
                if (dropTables == null) {
                    dropTables = new ArrayList<>();
                    ret.setAttribute(HiveEntity.KEY_DROP_TABLE, dropTables);
                }
                dropTables.add(tblQName);
            }
        }
        return ret;
    }
}
