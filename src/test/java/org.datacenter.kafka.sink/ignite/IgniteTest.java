package org.datacenter.kafka.sink.ignite;

import org.junit.Test;

public class IgniteTest {

    @Test
    public void testExecDdlSql() {

        DataGrid.SINK.init("e:\\app\\ignite-9f.xml");

        try {
            DataGrid.SINK.execDdlSql(
                    "CREATE TABLE `db51014_bee_dev_sys_user3` (\n"
                            + "  id int,\n"
                            + "  username varchar,\n"
                            + "  password varchar,\n"
                            + "  status int,\n"
                            + "  test1 varchar,\n"
                            + "  test2 varchar,\n"
                            + "  PRIMARY KEY (id)\n"
                            + ") WITH \"template=partitioned,backups=1,affinity_key=id,WRITE_SYNCHRONIZATION_MODE=PRIMARY_SYNC, CACHE_NAME=db51014_bee_dev_sys_user3,KEY_TYPE=db51014_bee_dev_sys_user3.Key,VALUE_TYPE=db51014_bee_dev_sys_user3.Value\";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
