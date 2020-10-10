package org.sen.hive.events;


import org.sen.hive.HiveHookContext;
import org.sen.hive.entity.HiveEntity;

/**
 * @author
 */
public class DefaultEvent extends BaseHiveEvent {
    public DefaultEvent(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_DEFAULT);
        return context.toJson(ret.getResult());
    }
}
