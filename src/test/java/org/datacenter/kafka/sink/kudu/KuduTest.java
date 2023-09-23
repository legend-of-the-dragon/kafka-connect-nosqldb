package org.datacenter.kafka.sink.kudu;

import org.apache.kudu.client.*;
import org.datacenter.kafka.sink.exception.DbDdlException;

import java.util.ArrayList;
import java.util.Collections;

public class KuduTest {

    private  KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduClient getKuduClient() {

        if (kuduClient == null) {
            String[] kuduMasters = "data11.bigdata.9f.cn:7051,data12.bigdata.9f.cn:7051,data13.bigdata.9f.cn:7051,data14.bigdata.9f.cn:7051,data15.bigdata.9f.cn:7051".split(",");
            java.util.ArrayList<String> kuduMasterList = new ArrayList<>(kuduMasters.length);
            Collections.addAll(kuduMasterList, kuduMasters);
            return new KuduClient.KuduClientBuilder(kuduMasterList).build();
        } else {
            return kuduClient;
        }
    }

    private KuduSession getKuduSession() {

        if (kuduSession == null || kuduSession.isClosed()) {
            kuduSession = getKuduClient().newSession();
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        }
        return kuduSession;
    }

    private KuduTable getKuduTable(String tableName) {
        KuduTable kuduTable;
        try {
            kuduTable = getKuduClient().openTable(tableName);
        } catch (KuduException e) {
            throw new DbDdlException("获取kudu表异常.", e);
        }
        return kuduTable;
    }

    public void testScanForTimestamp(){

        getKuduSession();

        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder("data11.bigdata.9f.cn:7051,data12.bigdata.9f.cn:7051,data13.bigdata.9f.cn:7051,data14.bigdata.9f.cn:7051,data15.bigdata.9f.cn:7051");
        AsyncKuduClient asyncKuduClient = asyncKuduClientBuilder.build();
        KuduTable kuduTable = getKuduTable("");
        AsyncKuduScanner.AsyncKuduScannerBuilder asyncKuduScannerBuilder = asyncKuduClient.newScannerBuilder(kuduTable);
        AsyncKuduScanner asyncKuduScanner = asyncKuduScannerBuilder.
                readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT).
                snapshotTimestampRaw(1L).build();


    }
}
