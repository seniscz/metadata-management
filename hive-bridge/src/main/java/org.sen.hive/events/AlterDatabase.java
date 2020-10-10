package org.sen.hive.events;


import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;

/**
 * @author
 */
public class AlterDatabase extends CreateDatabase {

    public AlterDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
