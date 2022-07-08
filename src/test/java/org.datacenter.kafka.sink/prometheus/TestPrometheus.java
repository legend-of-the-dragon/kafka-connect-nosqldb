package org.datacenter.kafka.sink.prometheus;

import com.bdwise.prometheus.client.builder.InstantQueryBuilder;
import com.bdwise.prometheus.client.builder.QueryBuilderType;
import com.bdwise.prometheus.client.converter.ConvertUtil;
import com.bdwise.prometheus.client.converter.query.DefaultQueryResult;
import com.bdwise.prometheus.client.converter.query.VectorData;
import org.datacenter.kafka.sink.ignite.ElasticLimit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestPrometheus {

    private static final String TARGET_SERVER = "http://10.255.200.100:9090/";

    @Before
    public void setUp() throws Exception {}

    @Test
    public void testGetStripedExecutorTotalQueueSizeByInstance() throws IOException {

        InstantQueryBuilder iqb = QueryBuilderType.InstantQuery.newInstance(TARGET_SERVER);
        URI targetUri =
                iqb.withQuery("max(sum(threadPools_StripedExecutor_TotalQueueSize{}) by (instance))")
                        .build();

        String resultString = ElasticLimit.executePrometheusHttpApi(targetUri);

        System.out.println(resultString);

        DefaultQueryResult<VectorData> result = ConvertUtil.convertQueryResultString(resultString);
        for (VectorData vectorData : result.getResult()) {
            System.out.println(
                    String.format(
                            "%s %s %10.2f",
                            vectorData.getMetric().get("instance"),
                            vectorData.getFormattedTimestamps("yyyy-MM-dd hh:mm:ss"),
                            vectorData.getValue()));
        }

        System.out.println(result);
    }

    @Test
    public void testDiv(){
        System.out.println(((15.0*10000)/1000)/100);
    }
}
