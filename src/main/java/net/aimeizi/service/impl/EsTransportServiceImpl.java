package net.aimeizi.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import net.aimeizi.Common.BeanUtil;
import net.aimeizi.client.elasticsearch.EsManager;
import net.aimeizi.model.Article;
import net.aimeizi.service.ArticleService;
import net.aimeizi.service.EsTransportService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.springframework.stereotype.Service;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2015/9/21.
 */
@Service
public class EsTransportServiceImpl implements EsTransportService {

    @Override
    public<T> Map<String, Object> search(Class<T> classz, String index, String type, String field, String queryString, int pageNumber, int pageSize) throws Exception {
    //public<T> Map<String, Object> search(Class<T> classz, String index, String type, Map<String, Object> searchMap, int pageNumber, int pageSize) throws Exception {
        //获取查询key和value
//        Set<String> keySet = searchMap.keySet();
//        for (String key : keySet) {
//            //key value 值对应关系
//            System.out.println(key+":"+searchMap.get(key));
//        }
        //System.out.println("{querySearch:{index:"+index + ", type:" +type +", field:"+field + ", queryString:" +queryString+"}}");
        EsManager.getInstance().init();
        //List<Map<String, Object>> list = new ArrayList<>();
        List<T> list = new ArrayList<>();
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
                .setQuery(QueryBuilders.matchQuery(field, queryString))

                .highlighter(highlightBuilder)
                .setFrom((pageNumber - 1) * pageSize)//设置起始页
                .setSize(pageSize)//设置页大小
                // 设置是否按查询匹配度排序
                .setExplain(true)
                // 最后就是返回搜索响应信息
                .execute()
                .actionGet();
        SearchHits searchHits = response.getHits();
        System.out.println("-----------------在["+field+"]中搜索关键字["+queryString+"]---------------------");
        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //获取高亮的字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get(field);
            // System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());
            Map<String, Object> sourceAsMap = searchHit.getSource();
            ObjectMapper mapper = new ObjectMapper();
            String jsonstr = mapper.writeValueAsString(sourceAsMap);//序列化sourceAsMap
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);//注意：此设置可以使user中属性可以比json中key少
            T object = mapper.readValue(jsonstr, classz); //反序列化user
            list.add((T) object);

        }
        if(list != null) {
            Map<String, Object> maps = new HashMap<String, Object>();
            maps.put("articles", list);
            maps.put("count", searchHits.getTotalHits());
            maps.put("took", response.getTook());
            return maps;
        }else{
            return null;
        }
    }

}
