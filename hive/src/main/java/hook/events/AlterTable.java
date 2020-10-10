package hook.events;


import hook.HiveHookContext;
import hook.entity.HiveEntity;

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
