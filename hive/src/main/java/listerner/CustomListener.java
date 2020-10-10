package listerner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @ClassName CustomListener
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/21 18:54
 * @Version 1.0
 **/
public class CustomListener extends MetaStoreEventListener {
    private static final Logger logger = LoggerFactory.getLogger(CustomListener.class);
    private static final ObjectMapper objMapper = new ObjectMapper();


    public CustomListener(Configuration config) {
        super(config);
        logWithHeader(" created ");
    }

    private void logWithHeader(Object obj) {
        logger.info("[CustomListener][Thread: " + Thread.currentThread().getName() + "] | " + objToStr(obj));
    }

    private String objToStr(Object obj) {
        try {
            return objMapper.writeValueAsString(obj);
        } catch (IOException e) {
            logger.error("Error on conversion", e);
        }
        return null;
    }

    @Override
    public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
        super.onConfigChange(tableEvent);
    }

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        super.onCreateTable(tableEvent);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        super.onDropTable(tableEvent);
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        super.onAlterTable(tableEvent);
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        super.onAddPartition(partitionEvent);
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        super.onDropPartition(partitionEvent);
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        super.onAlterPartition(partitionEvent);
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        super.onCreateDatabase(dbEvent);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        super.onDropDatabase(dbEvent);
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        super.onLoadPartitionDone(partSetDoneEvent);
    }

    @Override
    public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
        super.onAddIndex(indexEvent);
    }

    @Override
    public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
        super.onDropIndex(indexEvent);
    }

    @Override
    public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
        super.onAlterIndex(indexEvent);
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {
        super.onInsert(insertEvent);
    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);
    }
}
