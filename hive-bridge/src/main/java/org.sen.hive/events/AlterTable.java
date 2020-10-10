package org.sen.hive.events;


import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;

/**
 * @author
 */
public class AlterTable extends CreateTable {
    public AlterTable(HiveHookContext context) {
        super(context, true);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
