package org.sen.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.utils.SecurityUtils;
import org.sen.hive.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName HiveMetastoreHookImpl
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/27 16:10
 * @Version 1.0
 **/
public class HiveMetastoreHookImpl extends MetaStoreEventListener {
    private static final Logger logger = LoggerFactory.getLogger(HiveMetastoreHookImpl.class);
    private final HiveHook hiveHook;
    private final HiveMetastoreHook hook;

    public HiveMetastoreHookImpl(Configuration config, HiveHook hiveHook, HiveMetastoreHook hook) {
        super(config);
        this.hiveHook = hiveHook;
        this.hook = hook;
    }

    /**
     * 成员内部类
     */
    public class HiveMetastoreHook{
        public HiveMetastoreHook() {
        }

        public void handleEvent(HiveOperationContext operContext) {
            ListenerEvent listenerEvent = operContext.getEvent();

            if (!listenerEvent.getStatus()) {
                return;
            }

            try {
                HiveOperation oper = operContext.getOperation();
                HiveHookContext context = new HiveHookContext(hiveHook, oper, HiveHook.getKnownObjects(),
                        this, listenerEvent);
                BaseHiveEvent event = null;

                switch (oper) {
                    case CREATEDATABASE:
                        event = new CreateDatabase(context);
                        break;

                    case DROPDATABASE:
                        event = new DropDatabase(context);
                        break;

                    case ALTERDATABASE:
                        event = new AlterDatabase(context);
                        break;

                    case CREATETABLE:
                        event = new CreateTable(context, true);
                        break;

                    case DROPTABLE:
                        event = new DropTable(context);
                        break;

                    case ALTERTABLE_PROPERTIES:
                        event = new AlterTable(context);
                        break;

                    case ALTERTABLE_RENAME:
                        event = new AlterTableRename(context);
                        break;

                    case ALTERTABLE_RENAMECOL:
                        FieldSchema columnOld = operContext.getColumnOld();
                        FieldSchema columnNew = operContext.getColumnNew();

                        event = new AlterTableRenameCol(columnOld, columnNew, context);
                        break;

                    default:
                        if (logger.isDebugEnabled()) {
                            logger.debug("HiveMetastoreHook.handleEvent({}): operation ignored.", listenerEvent);
                        }
                        break;
                }

                if (event != null) {
                    final UserGroupInformation ugi = SecurityUtils.getUGI() == null ? Utils.getUGI() : SecurityUtils.getUGI();
                    //将数据发送出去
                    super.notifyEntities(event.getNotificationMessages(), ugi);
                }
            } catch (Throwable t) {
                logger.error("HiveMetastoreHook.handleEvent({}): failed to process operation {}", listenerEvent, t);
            }
        }
    }
}
