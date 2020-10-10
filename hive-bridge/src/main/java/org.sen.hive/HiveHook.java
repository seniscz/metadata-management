package org.sen.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.sen.hive.events.*;
import org.sen.hive.notification.NotificationInterface;
import org.sen.hive.notification.NotificationProvider;
import org.sen.hive.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @ClassName HiveHook
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/27 14:41
 * @Version 1.0
 **/
public class HiveHook implements ExecuteWithHookContext {
    private static final Logger logger = LoggerFactory.getLogger(HiveHook.class);

    protected static NotificationInterface notificationInterface;
    /**
     * key 为operationName ，value 为operation的值
     */
    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();
    private final JsonMapper jsonMapper;
    private ExecuteWithHookContext hiveHookImpl = null;
    private static HiveHookObjectNamesCache knownObjects = null;

    static {
        notificationInterface = NotificationProvider.get();
        //获取hive的操作语句
        for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }
    }

    public HiveHook() {
        this.jsonMapper = new JsonMapper();
    }

    /**
     * @param hookContext hive hook 的上下文
     * @throws Exception
     */
    @Override
    public void run(final HookContext hookContext) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("HiveHook run() " + hookContext.getOperationName());
        }
        try {
            //获得operation的值
            HiveOperation oper = OPERATION_MAP.get(hookContext.getOperationName());
            //获得 hive hook 的上下文
            HiveHookContext context = new HiveHookContext(oper, hookContext, jsonMapper);

            BaseHiveEvent event = null;

            switch (oper) {
                case CREATEDATABASE:
                    event = new CreateDatabase(context);
                    break;
                case DROPDATABASE:
                    event = new DropDatabase(context);
                    break;

                case ALTERDATABASE:
                case ALTERDATABASE_OWNER:
                    event = new AlterDatabase(context);
                    break;

                case CREATETABLE:
                    event = new CreateTable(context, true);
                    break;

                case DROPTABLE:
                case DROPVIEW:
                    event = new DropTable(context);
                    break;

                case ALTERTABLE_ADDPARTS:
                case CREATETABLE_AS_SELECT:
                case CREATEVIEW:
                case ALTERVIEW_AS:
                case LOAD:
                case EXPORT:
                case IMPORT:
                case QUERY:
                case TRUNCATETABLE:
                    event = new CreateHiveProcess(context);
                    break;

                case ALTERTABLE_DROPPARTS:
                case ALTERTABLE_FILEFORMAT:
                case ALTERTABLE_CLUSTER_SORT:
                case ALTERTABLE_BUCKETNUM:
                case ALTERTABLE_PROPERTIES:
                case ALTERVIEW_PROPERTIES:
                case ALTERTABLE_SERDEPROPERTIES:
                case ALTERTABLE_SERIALIZER:
                case ALTERTABLE_ADDCOLS:
                case ALTERTABLE_REPLACECOLS:
                case ALTERTABLE_PARTCOLTYPE:
                case ALTERTABLE_LOCATION:
                    event = new AlterTable(context);
                    break;

                case ALTERTABLE_RENAME:
                case ALTERVIEW_RENAME:
                    event = new AlterTableRename(context);
                    break;

                case ALTERTABLE_RENAMECOL:
                    event = new AlterTableRenameCol(context);
                    break;

                case SWITCHDATABASE:
                case SHOWDATABASES:
                case SHOWTABLES:
                case SHOW_CREATETABLE:
                case SHOWCOLUMNS:
                case SHOWPARTITIONS:
                case SHOWFUNCTIONS:
                case SHOW_TABLESTATUS:
                case SHOW_TBLPROPERTIES:
                case SHOWLOCKS:
                case DESCDATABASE:
                case DESCTABLE:
                case DESCFUNCTION:
                    if (logger.isDebugEnabled()) {
                        logger.debug("HiveHook run(), process operation {}", hookContext.getOperationName());
                    }
                    break;

                default:
                    event = new DefaultEvent(context);
                    break;
            }

            if (event != null) {
                String message = event.getNotificationMessages();
                if (StringUtils.isNotBlank(message)) {
                    notificationInterface.send(message);
                }
            }
        } catch (Throwable t) {
            logger.error("HiveHook run(), failed to process operation {}", hookContext.getOperationName(), t);
        }
    }


    /**
     * 获取 Hive Object Names
     *
     * @return
     */
    public static HiveHookObjectNamesCache getKnownObjects() {
        if (knownObjects != null && knownObjects.isCacheExpired()) {
            logger.info("HiveHook.run(): purging cached databaseNames ({}) and tableNames ({})",
                    knownObjects.getCachedDbCount(), knownObjects.getCachedTableCount());

            knownObjects = new HiveHook.HiveHookObjectNamesCache(10000,
                    10000, 60 * 60);
        }

        return knownObjects;
    }

    /**
     * 内部类
     */
    public static class HiveHookObjectNamesCache {
        /**
         * db 的数量
         */
        private final int dbMaxCacheCount;
        /**
         * table 的数量
         */
        private final int tblMaxCacheCount;
        /**
         * 缓存的时间
         */
        private final long cacheExpiryTimeMs;
        /**
         * 已经存在的 db
         */
        private final Set<String> knownDatabases;
        /**
         * 已经存在的table
         */
        private final Set<String> knownTables;

        /**
         * @param dbMaxCacheCount
         * @param tblMaxCacheCount
         * @param nameCacheRebuildIntervalSeconds 缓存重建间隔
         */
        public HiveHookObjectNamesCache(int dbMaxCacheCount, int tblMaxCacheCount, long nameCacheRebuildIntervalSeconds) {
            this.dbMaxCacheCount = dbMaxCacheCount;
            this.tblMaxCacheCount = tblMaxCacheCount;
            this.cacheExpiryTimeMs = nameCacheRebuildIntervalSeconds <= 0 ? Long.MAX_VALUE : (System.currentTimeMillis() + (nameCacheRebuildIntervalSeconds * 1000));
            //对象加锁
            this.knownDatabases = Collections.synchronizedSet(new HashSet<>());
            this.knownTables = Collections.synchronizedSet(new HashSet<>());
        }

        public int getCachedDbCount() {
            return knownDatabases.size();
        }

        public int getCachedTableCount() {
            return knownTables.size();
        }

        /**
         * 缓存是否过期
         *
         * @return
         */
        public boolean isCacheExpired() {
            return System.currentTimeMillis() > cacheExpiryTimeMs;
        }

        /**
         * 该库是否已经存在
         *
         * @param dbQualifiedName
         * @return
         */
        public boolean isKnownDatabase(String dbQualifiedName) {
            return knownDatabases.contains(dbQualifiedName);
        }

        public boolean isKnownTable(String tblQualifiedName) {
            return knownTables.contains(tblQualifiedName);
        }


        public void addToKnownDatabase(String dbQualifiedName) {
            if (knownDatabases.size() < dbMaxCacheCount) {
                knownDatabases.add(dbQualifiedName);
            }
        }

        public void addToKnownTable(String tblQualifiedName) {
            if (knownTables.size() < tblMaxCacheCount) {
                knownTables.add(tblQualifiedName);
            }
        }

        /**
         * 删除该 db
         *
         * @param dbQualifiedName
         */
        public void removeFromKnownDatabase(String dbQualifiedName) {
            knownDatabases.remove(dbQualifiedName);
        }

        /**
         * 删除该 table
         *
         * @param tblQualifiedName
         */
        public void removeFromKnownTable(String tblQualifiedName) {
            if (tblQualifiedName != null) {
                knownTables.remove(tblQualifiedName);
            }
        }
    }
}
