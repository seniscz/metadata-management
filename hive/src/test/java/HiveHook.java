import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;

/**
 * @ClassName HiveHook
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/21 11:38
 * @Version 1.0
 **/
public class HiveHook {

    public static void main(String[] args) {
        HiveConf hiveConf = new HiveConf();
        //hiveConf.addResource("hive-site.xml");
        hiveConf.set("hive.metastore.uris", "thrift://dev-kafka03:9083,thrift://dev-kudu02:9083,thrift://dev-master:9083");
        try {
            System.out.println("===hiveConf ======>" + hiveConf.getAllProperties());

            HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

            hiveMetaStoreClient.setMetaConf("hive.metastore.client.capability.check","false");
            List<String> tablesList = hiveMetaStoreClient.getAllTables("default");
            System.out.print("default 库下面所有的表:  ");
            for (String str : tablesList) {
                System.out.print(str + "\t");
            }
            System.out.println();
            //获取表信息
            System.out.println("default.user_info 表信息: ");
            Table table = hiveMetaStoreClient.getTable("default", "user_info");
            List<FieldSchema> fieldSchemaList = table.getSd().getCols();
            for (FieldSchema schema : fieldSchemaList) {
                System.out.println("字段: " + schema.getName() + ", 类型: " + schema.getType());
            }
            hiveMetaStoreClient.close();

        } catch (MetaException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

    }
}
