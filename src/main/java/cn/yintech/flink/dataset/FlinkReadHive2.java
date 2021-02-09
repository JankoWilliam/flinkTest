package cn.yintech.flink.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkReadHive2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .build());

        //构造hive catalog
        String name = "myhive";      // Catalog名称，定义一个唯一的名称表示
        String defaultDatabase = "default";  // 默认数据库名称
        String hiveConfDir = "/home/licaishi/lib/flink-hive-conf";  // hive-site.xml路径
        String version = "2.1.1";       // Hive版本号


        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog("myhive", hive);
        tEnv.useCatalog("myhive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("dwd");

        Table table = tEnv.sqlQuery("SELECT * FROM dwd.dwd_base_event_1d where dt = '2020-10-22' LIMIT 10");


        tEnv.execute("FlinkReadHive2");

    }
}
