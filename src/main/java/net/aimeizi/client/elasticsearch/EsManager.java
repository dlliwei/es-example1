package net.aimeizi.client.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EsManager {
    private static EsManager mEsManager = null;
    private EsManager(){}
    public static synchronized EsManager getInstance(){
        if(mEsManager == null)
            mEsManager = new EsManager();
        return mEsManager;
    }
    private TransportClient client;
    private BulkProcessor bulkProcessor;
    public TransportClient getClient(){
        return client;
    }

    public  boolean init() {
        String clusterName = "elasticsearch";
        String userName = "elastic";
        String pwd = "changeme";

        boolean result = false;
        if (client == null){
            System.out.println( "init elasticsearch, clusterName=" +clusterName + ", user:pwd=" + userName + ":" + pwd);
            try {

                Settings settings = Settings.builder().put("cluster.name", clusterName)
                        .put("client.transport.sniff", true)
                        //.put("xpack.security.transport.ssl.enabled", false)
                        .put("xpack.security.user", userName+":"+pwd)
                        .build();
                client = new PreBuiltXPackTransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
                /*
                client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
                 */

            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }else{
            System.out.println( "already init elasticsearch, clusterName=" +clusterName + ", user:pwd=" + userName + ":" + pwd);
        }

        if (client != null){
            result = true;

        }
        return result;
    }
    public void destroy(){
        if (client != null){
            client.close();
            client = null;
        }
        if (bulkProcessor != null){
            bulkProcessor.close();
            bulkProcessor = null;
        }
    }
    public BulkProcessor getBulkProcessor() {
        //如果消息数量到达1000 或者消息大小到大5M 或者时间达到5s 任意条件满足，客户端就会把当前的数据提交到服务端处理
        //调用：bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));
        if(bulkProcessor == null) {
            bulkProcessor = BulkProcessor.builder(
                    client,
                    new BulkProcessor.Listener() {
                        public void beforeBulk(long l, BulkRequest bulkRequest) {
                            System.out.println("---尝试插入{}条数据---" + bulkRequest.numberOfActions());
                        }

                        public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                            System.out.println("---尝试插入{}条数据成功---" + bulkRequest.numberOfActions());
                        }

                        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                            System.out.println("[es错误]---尝试插入数据失败---" + throwable);
                        }
                    })
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(1)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
        }
        return bulkProcessor;
    }
}
