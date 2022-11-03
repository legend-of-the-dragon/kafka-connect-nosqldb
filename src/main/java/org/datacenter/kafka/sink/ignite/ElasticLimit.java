package org.datacenter.kafka.sink.ignite;

import com.bdwise.prometheus.client.builder.InstantQueryBuilder;
import com.bdwise.prometheus.client.builder.QueryBuilderType;
import com.bdwise.prometheus.client.converter.ConvertUtil;
import com.bdwise.prometheus.client.converter.query.DefaultQueryResult;
import com.bdwise.prometheus.client.converter.query.VectorData;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class ElasticLimit {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticLimit.class);

    private static final Semaphore elasticLimitLock = new Semaphore(1);

    private static Map<String, Pair<Boolean, Long>> elasticLimitMap = new HashMap<>();

    /** 最后进行超速检测的时间 */
    private static Long lastComputeSpeedTime = -1L;

    /**
     * 计算connectorName是否需要限速以及限速的时长(ms)，限速持续5分钟
     *
     * @return 是否需要限速，线程睡眠时长， (tasks.max、parallel.ops未来从连接器外来调整，需要重启连接器)
     */
    public static Pair<Boolean, Long> getElasticLimit(String connectorName) {

        // 5分钟计算一次
        long now = System.currentTimeMillis();
        if (lastComputeSpeedTime == -1 || (now - lastComputeSpeedTime) / 1000 / 60 >= 5) {

            // 加锁，防止初始化或者某些特殊时刻重复请求
            try {
                elasticLimitLock.acquire();
                now = System.currentTimeMillis();
                if (lastComputeSpeedTime == -1 || (now - lastComputeSpeedTime) / 1000 / 60 >= 5) {
                    elasticLimitMap.clear();
                    boolean speed = isSpeed();
                    if (speed) {
                        elasticLimitMap = increaseConnectorNameTop();
                        LOG.info("当前已经超速，需要进行限速,限速的结果如下:{}", elasticLimitMap);
                    }
                    lastComputeSpeedTime = now;
                }
            } catch (InterruptedException e) {
                LOG.error("限速计算异常.", e);
            } finally {
                elasticLimitLock.release();
            }
        }

        Pair<Boolean, Long> pair = elasticLimitMap.get(connectorName);
        if (pair == null) {
            pair = ImmutablePair.of(false, 0L);
        }

        return pair;
    }

    /**
     * 增长量占据所有连接器5%以上的主题
     *
     * @return
     */
    private static Map<String, Pair<Boolean, Long>> increaseConnectorNameTop() {

        // 1、query prometheus
        List<VectorData> vectorDataList =
                executePrometheusInstantQuery(
                        "sum( increase   (kafka_connect_sink_task_metrics_sink_record_read_total{connector=~\"sink_ig_.*\"}[30m])) by (connector)");

        // 2、get、sum connectorName and increase from result
        double sum = 0;
        List<Pair<String, Double>> connectorIncreaseList = new ArrayList<>();
        for (VectorData vectorData : vectorDataList) {
            String connector = vectorData.getMetric().get("connector");
            double value = vectorData.getValue();
            connectorIncreaseList.add(Pair.of(connector, value));
            sum += value;
        }
        // 4、get elastic limit config from ignite

        // 5、计算每个connectorName占比，超过5%的根据配置计算需要限速时长(ms)
        for (Pair<String, Double> connectorIncrease : connectorIncreaseList) {
            String connectorName = connectorIncrease.getKey();
            double percentage = ((connectorIncrease.getValue() * 10000.0) / sum) / 100.0;
            if (percentage > 5) {
                long limitNum = 25000;
                elasticLimitMap.put(connectorName, Pair.of(true, limitNum));
            }
        }

        return elasticLimitMap;
    }

    /**
     * 检查集群是否超速
     *
     * @return
     */
    private static boolean isSpeed() {

        // 1、query prometheus (gc、线程数、检查点写入时间、tasks.max、consumer topic lag)
        double maxExecutorTotalQueueSize =
                getPrometheusDoubleResult("max(threadPools_StripedExecutor_TotalQueueSize{})");
        double maxLastCheckpointDuration =
                getPrometheusDoubleResult("max(io_datastorage_LastCheckpointDuration)");
        double maxIrateJvmGcTotalDuration =
                getPrometheusDoubleResult("max(irate(ignite_longJVMPausesTotalDuration{}[10m]))");

        // 2、get verify threshold value config from ignite
        // 线程池积压阈值：100万
        double thresholdExecutorTotalQueueSize = 1000000;
        // 检查点写入时长阈值：200s
        double thresholdLastCheckpointDuration = 200000;
        // gc总时长增速阈值：500
        double thresholdIrateJvmGcTotalDuration = 500;

        // 3、verify
        if (maxExecutorTotalQueueSize > thresholdExecutorTotalQueueSize
                || maxLastCheckpointDuration > thresholdLastCheckpointDuration
                || maxIrateJvmGcTotalDuration > thresholdIrateJvmGcTotalDuration) {
            return true;
        } else {
            return false;
        }
    }

    public static double getPrometheusDoubleResult(String queryString) {
        List<VectorData> vectorDataList = executePrometheusInstantQuery(queryString);
        if (vectorDataList.size() == 1) {
            VectorData vectorData = vectorDataList.get(0);
            return vectorData.getValue();
        } else {
            return -1;
        }
    }

    public static String prometheusServer = "http://172.30.80.70:9090/";

    public static List<VectorData> executePrometheusInstantQuery(String queryString) {

        List<VectorData> results = null;

        InstantQueryBuilder iqb = QueryBuilderType.InstantQuery.newInstance(prometheusServer);
        URI targetUri = iqb.withQuery(queryString).build();

        try {
            String resultString = ElasticLimit.executePrometheusHttpApi(targetUri);
            DefaultQueryResult<VectorData> result =
                    ConvertUtil.convertQueryResultString(resultString);
            results = result.getResult();
        } catch (IOException e) {
            LOG.error("execute PrometheusSimpleVectorQuery error.", e);
        }
        return results;
    }

    private static CloseableHttpClient client = HttpClients.createDefault();

    public static String executePrometheusHttpApi(URI targetUri) throws IOException {

        HttpGet httpGet = new HttpGet(targetUri);
        // 设置请求头信息，鉴权(没有可忽略)
        //        httpGet.setHeader("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
        // 设置配置请求参数(没有可忽略)
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(5000) // 连接主机服务超时时间
                        .setConnectionRequestTimeout(5000) // 请求超时时间
                        .setSocketTimeout(10000) // 数据读取超时时间
                        .build();
        // 为httpGet实例设置配置
        httpGet.setConfig(requestConfig);
        // 执行请求
        CloseableHttpResponse response = client.execute(httpGet);
        // 获取Response状态码
        //        int statusCode = response.getStatusLine().getStatusCode();
        // 获取响应实体, 响应内容
        HttpEntity entity = response.getEntity();
        // 通过EntityUtils中的toString方法将结果转换为字符串
        String resultString = EntityUtils.toString(entity);
        response.close();

        return resultString;
    }

    public static void closeHttpClient() {
        try {
            client.close();
        } catch (IOException e) {
            LOG.error("关闭httpclient异常.", e);
        }
    }
}
