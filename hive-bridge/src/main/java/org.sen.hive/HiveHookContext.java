package org.sen.hive;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.sen.hive.entity.HiveEntity;
import org.sen.hive.entity.MRTask;
import org.sen.hive.events.BaseHiveEvent;
import org.sen.hive.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HiveHookContext
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 16:54
 * @Version 1.0
 **/
public class HiveHookContext {
    private static final Logger logger = LoggerFactory.getLogger(HiveHookContext.class);

    public static final char QNAME_SEP_ENTITY_NAME = '.';
    public static final String TEMP_TABLE_PREFIX = "_temp-";

    /**
     * Json parser
     */
    private JsonMapper jsonMapper;
    /**
     * Hive operation
     */
    private final HiveOperation hiveOperation;
    /**
     * Hive context
     */
    private final HookContext hiveContext;
    /**
     * Cache entity
     */
    private Map<String, Map<String, Object>> entityMap;
    /**
     * Cache columns
     */
    private Map<String, List<String>> columnMap;
    private HiveHook hook;
    private Hive hive;
    private HiveHook.HiveHookObjectNamesCache knownObjects;


    public HiveHookContext(HiveHook hook, HiveOperation hiveOperation, HiveHook.HiveHookObjectNamesCache knownObjects,
                           HiveMetastoreHookImpl.HiveMetastoreHook metastoreHook, ListenerEvent listenerEvent) throws Exception {
        this(hook, hiveOperation, null, knownObjects, listenerEvent);
    }

    public HiveHookContext(HiveOperation hiveOperation, HookContext hookContext, JsonMapper jsonMapper) {
        this.hiveOperation = hiveOperation;
        this.hiveContext = hookContext;
        this.jsonMapper = jsonMapper;
    }

    /**
     * Constructor
     *
     * @param hiveOperation
     * @param hiveContext
     * @param
     * @throws Exception
     */
    public HiveHookContext(HiveHook hiveHook, HiveOperation hiveOperation, HookContext hiveContext,
                           HiveHook.HiveHookObjectNamesCache knownObjects, ListenerEvent listenerEvent) throws Exception {
        this.hook = hiveHook;
        this.hiveOperation = hiveOperation;
        this.hiveContext = hiveContext;
        this.hive = hiveContext != null ? Hive.get(hiveContext.getConf()) : null;
        this.knownObjects = knownObjects;
        this.entityMap = new HashMap<>();
        this.columnMap = new HashMap<>();
    }


    public String toJson(Object obj) {
        return jsonMapper.toJson(obj);
    }

    public HookContext getHiveContext() {
        return hiveContext;
    }

    public Hive getHive() throws HiveException {
        return Hive.get(hiveContext.getConf());
    }

    public HiveOperation getHiveOperation() {
        return hiveOperation;
    }

    public void putEntity(String qualifiedName, Map<String, Object> entity) {
        entityMap.put(qualifiedName, entity);
    }

    public Map<String, Object> getEntity(String qualifiedName) {
        return entityMap.get(qualifiedName);
    }

    public String getQualifiedName(Database db) {
        return db.getName().toLowerCase();
    }

    public String getQualifiedName(Table table) {
        String tableName = table.getTableName();

        if (table.isTemporary()) {
            if (SessionState.get() != null && SessionState.get().getSessionId() != null) {
                tableName = tableName + TEMP_TABLE_PREFIX + SessionState.get().getSessionId();
            } else {
                tableName = tableName + TEMP_TABLE_PREFIX + RandomStringUtils.random(10);
            }
        }

        return (table.getDbName() + QNAME_SEP_ENTITY_NAME + tableName).toLowerCase();
    }

    public String getHadoopJobName() {
        return hiveContext.getConf().getVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
    }

    public String getHistFileName() {
        return SessionState.get().getHiveHistory().getHistFileName();
    }

    public String getIpAddress() {
        return hiveContext.getIpAddress();
    }

    protected String getUserName() {
        String ret = getHiveContext().getUserName();

        if (StringUtils.isEmpty(ret)) {
            UserGroupInformation ugi = getHiveContext().getUgi();

            if (ugi != null) {
                ret = ugi.getShortUserName();
            }

            if (StringUtils.isEmpty(ret)) {
                try {
                    ret = UserGroupInformation.getCurrentUser().getShortUserName();
                } catch (IOException e) {
                    logger.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
                    ret = System.getProperty("user.name");
                }
            }
        }
        return ret;
    }

    /**
     * Get table column list from cache
     *
     * @param tableQualifiedName
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<String> getColumnByTableName(String tableQualifiedName) {
        String tableName = null;
        if (StringUtils.isNotEmpty(tableQualifiedName)) {
            tableName = tableQualifiedName.toLowerCase();
        }
        List<String> ret = columnMap.get(tableName);
        if (ret == null) {
            Map<String, Object> entity = getEntity(tableName);
            if (entity != null) {
                List<Map<String, Object>> columns =
                        (List<Map<String, Object>>) entity.get(BaseHiveEvent.ATTRIBUTE_COLUMNS);
                if (columns != null) {
                    ret = new ArrayList<>();
                    for (Map<String, Object> col : columns) {
                        String colName = (String) col.get(BaseHiveEvent.ATTRIBUTE_NAME);
                        ret.add(colName);
                    }
                    columnMap.put(tableName, ret);
                }
            }
        }
        return ret;
    }

    public Map<String, Object> getQueryInfo() {
        Map<String, Object> ret = new HashMap<>();
        List<MRTask> mrTasks = getMRTasks();
        if (!mrTasks.isEmpty()) {
            ret.put(HiveEntity.KEY_QUERY_MRTASKS, mrTasks);
        }
        return ret;
    }

    public String getQueryID() {
        return hiveContext.getQueryPlan().getQueryId();
    }

    public Long getQueryStartTime() {
        return hiveContext.getQueryPlan().getQueryStartTime();
    }

    public String getQueryStr(boolean encoding) {
        String ret = null;
        if (!encoding) {
            return hiveContext.getQueryPlan().getQueryStr().trim();
        }
        try {
            ret = URLEncoder.encode(
                    hiveContext.getQueryPlan().getQueryStr().trim(),
                    CharEncoding.UTF_8);
        } catch (UnsupportedEncodingException e) {
            ret = hiveContext.getQueryPlan().getQueryStr();
            logger.error(e.getMessage());
        }
        return ret;
    }

    public List<MRTask> getMRTasks() {
        List<MRTask> resVal = new ArrayList<>();
        List<ExecDriver> driverList = Utilities.getMRTasks(hiveContext.getQueryPlan().getRootTasks());
        if ((driverList != null) && !driverList.isEmpty()) {
            for (ExecDriver ed : driverList) {
                MRTask task = new MRTask();
                task.setId(ed.getId());
                task.setJobId(ed.getJobID());
                task.setName(ed.getName());
                MapredWork mapredWork = ed.getWork();
                if (mapredWork != null) {
                    Map<String, Object> workInfo = getWorkInfo(mapredWork);
                    if (!workInfo.isEmpty()) {
                        task.setWorkInfo(workInfo);
                    }
                }
                resVal.add(task);
            }
        }
        return resVal;
    }

    /**
     * Get HSQL on HADOOP work information
     *
     * @param mapredWork
     * @return
     */
    private Map<String, Object> getWorkInfo(MapredWork mapredWork) {
        Map<String, Object> workInfo = new HashMap<>();
        Statistics workStatistics = mapredWork.getStatistics();
        if (workStatistics != null) {
            setMapKeyValue(workInfo, HiveEntity.KEY_WORK_AVGROWSIZE, workStatistics.getAvgRowSize());
            setMapKeyValue(workInfo, HiveEntity.KEY_WORK_DATASIZE, workStatistics.getDataSize());
            setMapKeyValue(workInfo, HiveEntity.KEY_WORK_NUMROWS, workStatistics.getNumRows());
        }
        MapWork mapWork = mapredWork.getMapWork();
        if (mapWork != null) {
            Map<String, Object> mapWorkInfo = new HashMap<>();
            setMapKeyValue(mapWorkInfo, HiveEntity.KEY_WORK_NUMTASKS, mapWork.getNumMapTasks());
            Statistics mapStatistics = mapWork.getStatistics();
            if (mapStatistics != null) {
                setMapKeyValue(mapWorkInfo, HiveEntity.KEY_WORK_AVGROWSIZE, mapStatistics.getAvgRowSize());
                setMapKeyValue(mapWorkInfo, HiveEntity.KEY_WORK_DATASIZE, mapStatistics.getDataSize());
                setMapKeyValue(mapWorkInfo, HiveEntity.KEY_WORK_NUMROWS, mapStatistics.getNumRows());
            }
            if (!mapWorkInfo.isEmpty()) {
                workInfo.put(HiveEntity.KEY_MAPWORK_INFO, mapWorkInfo);
            }
        }
        ReduceWork reduceWork = mapredWork.getReduceWork();
        if (reduceWork != null) {
            Map<String, Object> reduceWorkInfo = new HashMap<>();
            setMapKeyValue(reduceWorkInfo, HiveEntity.KEY_WORK_NUMTASKS, reduceWork.getNumReduceTasks());
            Statistics reduceStatistics = reduceWork.getStatistics();
            if (reduceStatistics != null) {
                setMapKeyValue(reduceWorkInfo, HiveEntity.KEY_WORK_AVGROWSIZE, reduceStatistics.getAvgRowSize());
                setMapKeyValue(reduceWorkInfo, HiveEntity.KEY_WORK_DATASIZE, reduceStatistics.getDataSize());
                setMapKeyValue(reduceWorkInfo, HiveEntity.KEY_WORK_NUMROWS, reduceStatistics.getNumRows());
            }
            if (!reduceWorkInfo.isEmpty()) {
                workInfo.put(HiveEntity.KEY_REDUCEWORK_INFO, reduceWorkInfo);
            }
        }
        return workInfo;
    }

    private void setMapKeyValue(Map<String, Object> map, String key, Object value) {
        if (map != null) {
            if (StringUtils.isNotBlank(key) && (value != null)) {
                map.put(key, value);
            }
        }
    }

    private String getExecuterAddress() {
        String ret = null;
        try {
            ret = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            //ignore
        }
        return ret;
    }

    public HiveEntity createHiveEntity() {
        HiveEntity ret = new HiveEntity();
        ret.setCreatedBy(getUserName());
        ret.setCreateTime(System.currentTimeMillis());
        ret.setOperationName(getHiveContext().getOperationName());
        ret.setHadoopJobName(getHadoopJobName());
        ret.setIpAddress(getIpAddress());
        ret.setExecuterAddress(getExecuterAddress());
        ret.setQueryId(getQueryID());
        ret.setQueryStr(getQueryStr(true));
        ret.setQueryStartTime(getQueryStartTime());
        ret.setQueryInfo(getQueryInfo());
        return ret;
    }

}
