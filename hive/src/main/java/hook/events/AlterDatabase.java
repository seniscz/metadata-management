package hook.events;


import hook.HiveHookContext;
import hook.entity.HiveEntity;

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
