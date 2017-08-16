package net.aimeizi.client.elasticsearch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class Tools {
    public static void bulkAddIndexByMap(String index, String type, List<Map<String, Object>> listMap) {
        BulkRequestBuilder bulkRequest = EsManager.getInstance().getClient().prepareBulk();

        for (Map<String, Object> map: listMap) {
            IndexRequest indexRequest = new IndexRequest(index, type);
            indexRequest.source(map);
            bulkRequest.add(indexRequest);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }

    public static long deleteByQuery(String index, String key, String value) {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(EsManager.getInstance().getClient())
                        .filter(QueryBuilders.matchQuery(key, value))
                        .source(index)
                        .get();
//                        .execute(new ActionListener<BulkIndexByScrollResponse>() {
//                            @Override
//                            public void onResponse(BulkIndexByScrollResponse response) {
//                                long deleted = response.getDeleted();
//                            }
//                            @Override
//                            public void onFailure(Exception e) {
//                                // Handle the exception
//                            }
//                        });

        return response.getDeleted();
    }

    public static int updateByQuery(String index, String type, String key, Object value) {

        return 0;
    }

    public static List<Map<String, Object>> querySearch(String index, String type,String term, String queryString){
        System.out.println("{querySearch:{index:"+index + ", type:" +type +", term:"+term + ", queryString:" +queryString+"}}");

        List<Map<String, Object>> list = new ArrayList<>();
        // 设置高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("term");//高亮
        highlightBuilder.preTags("<em>").postTags("</em>");//高亮标签
        highlightBuilder.fragmentSize(200);//高亮内容长度

        SearchResponse response = EsManager.getInstance().getClient().prepareSearch(index)
                .setTypes(type)
                // 设置查询类型
// 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
// 2.SearchType.SCAN = 扫描查询,无序
// 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 设置查询关键词
                .setQuery(QueryBuilders.matchQuery(term, queryString))
                .highlighter(highlightBuilder)
                // 设置查询数据的位置,分页用
                .setFrom(0)
// 设置查询结果集的最大条数
                .setSize(60)
// 设置是否按查询匹配度排序
                .setExplain(true)
// 最后就是返回搜索响应信息
                .execute()
                .actionGet();
        SearchHits searchHits = response.getHits();
        System.out.println("-----------------在["+term+"]中搜索关键字["+queryString+"]---------------------");
        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
//获取高亮的字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get(term);
           // System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());
            Map<String, Object> source = searchHit.getSource();
            Set<String> keySet = source.keySet();
            for (String string : keySet) {
//key value 值对应关系
                System.out.println(string+":"+source.get(string));
            }
            list.add(source);
        }
        return list;
    }

//    public static List<User> MultGetTest(String index, String index2, String type, String type2) throws IOException {
//        //MultiGetRequestBuilder builder = EsManager.getInstance().getClient().prepareMultiGet();
//        MultiGetResponse multiGetItemResponses = EsManager.getInstance().getClient().prepareMultiGet()
//                .add(index, type, "1")
//                .add(index, type, "AV2bo_Ww2J5K9QSBXfsX", "AV2bpqyU2J5K9QSBXfv2", "AV2bo_Ww2J5K9QSBXfsW")
//                .add(index2, type, "AV2ZEg-ZQ5JZeRyX-fSE")
//                .get();
//        List<User> list = new ArrayList<User>();
//
//        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
//            GetResponse response = itemResponse.getResponse();
//            if (response.isExists()) {
//                String json = response.getSourceAsString();
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//                User user = mapper.readValue(json, User.class);
//                list.add(user);
//            }
//        }
//        return list;
//    }

    public static int addIndexByMap(Client client, String index, String type, Map<String, Object> map) {
        IndexResponse indexResponse = client.prepareIndex(index, type)
                .setSource(map)
                .execute()
                .actionGet();

        return indexResponse.status().getStatus();
    }

    public static void deleteIndex(String index) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        ActionFuture<DeleteIndexResponse> r = EsManager.getInstance().getClient().admin().indices().delete(deleteIndexRequest);
    }

    public static int deleteById(Client client, String index, String type, String id) {
        DeleteResponse deleteResponse = client.prepareDelete(index, type, id)
                .execute()
                .actionGet();
        return deleteResponse.status().getStatus();
    }
    /**
     * 判断一个index中的type是否有数据
     * @param index
     * @param type
     * @return
     * @throws Exception
     */
    public Boolean existDocOfType(String index, String type) throws Exception {
        SearchRequestBuilder builder = EsManager.getInstance().getClient().prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(1);
        SearchResponse response = builder.execute().actionGet();
        long docNum = response.getHits().getTotalHits();
        if (docNum == 0) {
            return false;
        }
        return true;
    }

    public static int upsertTest(String index, String type, String id) throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(index, type, id)
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("age", 12)
                        .field("name", "bb")
                        .endObject());

        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "bb")
                            .endObject())
                    .upsert(indexRequest);

        UpdateResponse response =  EsManager.getInstance().getClient().update(updateRequest).get();
        return response.status().getStatus();
    }
    //方法有问题
//    public static int upsert(String index, String type, String key, Object value, Object newValue) {
//        int result = 0;
//        try {
//            XContentBuilder builder = XContentFactory.jsonBuilder()
//                    .startObject()
//                    .field(key, value)
//                    .endObject();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        //先查找对应id
//        SearchResponse searchResponse = EsManager.getInstance().getClient().prepareSearch(index)
//                .setTypes(type)
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(termQuery(key, value))             // Query
//                //.setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
//                //.setFrom(0).setSize(60).setExplain(true)
//                .execute()
//                .actionGet();
//
//        System.out.println( "upsert.search result:" + searchResponse.status().getStatus());
//        List<User> list = new ArrayList<User>();
//        SearchHits hits = searchResponse.getHits();
//        SearchHit[] searchHits = hits.getHits();
//
//        //批量修改
//        XContentBuilder builder2 = null;
//        try {
//            builder2 = XContentFactory.jsonBuilder()
//                    .startObject()
//                    .field(key, newValue)
//                    .endObject();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        BulkRequestBuilder bulkRequest = EsManager.getInstance().getClient().prepareBulk();
//        for (SearchHit s : searchHits) {
//            String id = s.getId();
//            UpdateRequest updateRequest = new UpdateRequest(index, type, id);
//            updateRequest.doc(builder2);
//            bulkRequest.add(updateRequest);
//            result++;
//        }
//        BulkResponse bulkResponse = null;
//        try {
//            bulkResponse = bulkRequest.execute().actionGet();
//            if (bulkResponse.hasFailures()) {
//                // process failures by iterating through each bulk response item
//                System.out.println("");
//            }
//        }catch(Exception e){
//            System.out.println("");
//        }
//        return result;
//    }
    public static int updateIndexByBuilder(String index, String type, String id, String key, Object value) throws IOException, InterruptedException, ExecutionException {
        return updateIndexByBuilder(EsManager.getInstance().getClient(), index, type, id, key, value);
    }
    private static int updateIndexByBuilder(Client client, String index, String type, String id, String key, Object value) throws IOException, InterruptedException, ExecutionException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field(key, value)
                .endObject();

        UpdateResponse updateResponse = client.prepareUpdate(index, type, id)
                .setDoc(builder)
                .execute()
                .actionGet();

        return updateResponse.status().getStatus();
    }

    public static String getJsonFromIndex(String index, String type, String id) {
        return getJsonFromIndex(EsManager.getInstance().getClient(), index, type, id);
    }
    public static String getJsonFromIndex(Client client, String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id)
                .setOperationThreaded(false)
                .execute()
                .actionGet();

        return response.getSourceAsString();
    }

//    public static String query(String index, String type, String key, String value, int pagesize, int pageno) throws IOException {
//        return query(EsManager.getInstance().getClient(), index, type, key, value, pagesize, pageno);
//    }
//    private static String query(Client client, String index, String type, String key, String value, int pagesize, int pageno) throws IOException {
//        SearchResponse searchResponse = client.prepareSearch(index)
//                .setTypes(type)
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(termQuery(key, value))             // Query
//                //.setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
//                .setFrom((pageno - 1) * pagesize).setSize(pagesize).setExplain(true)
//                .execute()
//                .actionGet();
//
//        //以下操作是否可以优化？ 比如是否有个方法直接转换成list，不用执行一层for循环
//        List<User> list = new ArrayList<User>();
//        SearchHits hits = searchResponse.getHits();
//        SearchHit[] searchHits = hits.getHits();
//        for (SearchHit s : searchHits) {
//            String str = s.getSourceAsString().toString();
//            ObjectMapper mapper = new ObjectMapper();
//            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//            User user = mapper.readValue(str, User.class);
//            list.add(user);
//            System.out.println( "query result:" + str);
//        }
//        return "";
//    }
}
