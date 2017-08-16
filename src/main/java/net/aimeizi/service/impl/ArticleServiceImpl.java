package net.aimeizi.service.impl;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import net.aimeizi.model.Article;
import net.aimeizi.service.ArticleService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.stereotype.Service;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2015/9/21.
 */
@Service
public class ArticleServiceImpl implements ArticleService {


    private static JestClient getJestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://127.0.0.1:9200")
                .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
                .multiThreaded(true)
                .readTimeout(10000)
                .build());
        JestClient client = factory.getObject();
        return client;
    }

//    public Map<String, Object> searchByTransportClient(String term, String queryString, String older, int pageNumber, int pageSiz){
//        EsManager.getInstance().init();
//        List<Article> list = new ArrayList<>();
//        // 设置高亮字段
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.field("term");//高亮
//        highlightBuilder.preTags("<em>").postTags("</em>");//高亮标签
//        highlightBuilder.fragmentSize(200);//高亮内容长度
//
//        SearchResponse response = EsManager.getInstance().getClient().prepareSearch(Contacts.INDEX)
//                .setTypes(Contacts.TYPE)
//                // 设置查询类型
//// 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
//// 2.SearchType.SCAN = 扫描查询,无序
//// 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                // 设置查询关键词
//                .setQuery(QueryBuilders.matchQuery(term, queryString))
//                .highlighter(highlightBuilder)
//                // 设置查询数据的位置,分页用
//                .setFrom((pageNumber-1)*pageSiz)
//// 设置查询结果集的最大条数
//                .setSize(pageSiz)
//// 设置是否按查询匹配度排序
//                .setExplain(true)
//// 最后就是返回搜索响应信息
//                .execute()
//                .actionGet();
//        SearchHits searchHits = response.getHits();
//        System.out.println("-----------------在["+term+"]中搜索关键字["+queryString+"]---------------------");
//        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
//        SearchHit[] hits = searchHits.getHits();
//        for (SearchHit searchHit : hits) {
////获取高亮的字段
//            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
//            HighlightField highlightField = highlightFields.get(term);
//            // System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());
//            Map<String, Object> sourceAsMap = searchHit.sourceAsMap();
//            Set<String> keySet = sourceAsMap.keySet();
//            for (String string : keySet) {
////key value 值对应关系
//                System.out.println(string+":"+sourceAsMap.get(string));
//            }
//            Article a = null;
//            try {
//                a = (Article)mapToObject(sourceAsMap, Article.class);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            list.add(a);
//        }
//        Map<String, Object> maps = new HashMap<String, Object>();
//        maps.put("articles", list);
//        maps.put("count", response.getHits().totalHits);
//        maps.put("took", response.getTook());
//        return maps;
//    }


    /**
     * 检索
     *
     * @param field
     * @param queryString
     * @param older
     * @param pageNumber
     * @param pageSize
     * @return
     * @throws Exception
     */
    public Map<String, Object> search(String field, String queryString, String older, int pageNumber, int pageSize) throws Exception {
        List<Article> articles = new ArrayList<Article>();
        JestClient jestClient = getJestClient();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建查询
        if ("all".equals(field)) {
            searchSourceBuilder.query(QueryBuilders.queryStringQuery(queryString));
        } else {
            searchSourceBuilder.query(QueryBuilders.termQuery(field, queryString));
            // 设置排序
//            searchSourceBuilder.sort(field, "asc".equals(older) ? SortOrder.ASC : SortOrder.DESC); // 设置排序字段及排序顺序
        }
        // 设置高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");//高亮title
        highlightBuilder.field("content");//高亮content
        highlightBuilder.field("author");//高亮author
        highlightBuilder.field("source");//高亮source
        highlightBuilder.preTags("<em>").postTags("</em>");//高亮标签
        highlightBuilder.fragmentSize(200);//高亮内容长度
        searchSourceBuilder.highlighter(highlightBuilder);

        // 设置分页
        searchSourceBuilder.from((pageNumber - 1) * pageSize);//设置起始页
        searchSourceBuilder.size(pageSize);//设置页大小
        Search search = null;
        try {
            search = new Search.Builder(searchSourceBuilder.toString())
                    .addIndex("news")// 索引名称
                    .addType("news")
                    .build();
        }catch (NoClassDefFoundError e){
            e.getStackTrace();
        }catch (Exception e){
            e.getStackTrace();
        }

        SearchResult result = null;
        try {
            result = jestClient.execute(search);
        }catch (Exception e){
            e.getStackTrace();
        }
        // 手动解析
//        parseSearchResult(articles, result);

        // 自动解析
        JsonObject jsonObject = result.getJsonObject();
        JsonObject hitsobject = jsonObject.getAsJsonObject("hits");
        long took = jsonObject.get("took").getAsLong();
        long total = hitsobject.get("total").getAsLong();
        List<SearchResult.Hit<Article, Void>> hits = result.getHits(Article.class);
        for (SearchResult.Hit<Article, Void> hit : hits) {
            Article source = hit.source;
            //获取高亮后的内容
            Map<String, List<String>> highlight = hit.highlight;
            List<String> titlelist = highlight.get("title");//高亮后的title
            if (titlelist != null) {
                source.setTitle(titlelist.get(0));
            }
            List<String> contentlist = highlight.get("content");//高亮后的content
            if (contentlist != null) {
                source.setContent(contentlist.get(0));
            }
            List<String> authorlist = highlight.get("author");//高亮后的author
            if (authorlist != null) {
                source.setAuthor(authorlist.get(0));
            }
            List<String> sourcelist = highlight.get("source");//高亮后的source
            if (sourcelist != null) {
                source.setSource(sourcelist.get(0));
            }
            Article article = new Article();
            article.setId(source.getId());
            article.setTitle(source.getTitle());
            article.setContent(source.getContent());
            article.setSource(source.getSource());
            article.setAuthor(source.getAuthor());
            article.setUrl(source.getUrl());
            article.setPubdate(source.getPubdate());
            articles.add(article);
        }
        Map<String, Object> maps = new HashMap<String, Object>();
        maps.put("articles", articles);
        maps.put("count", total);
        maps.put("took", took);
        return maps;
    }

    /**
     * 手动解析查询结果
     * @param articles
     * @param result
     * @throws ParseException
     */
    private void parseSearchResult(List<Article> articles, SearchResult result) throws ParseException {
        JsonObject jsonObject = result.getJsonObject();
        long took = jsonObject.get("took").getAsLong();
        JsonObject hitsobject = jsonObject.getAsJsonObject("hits");
        long total = hitsobject.get("total").getAsLong();
        JsonArray jsonArray = hitsobject.getAsJsonArray("hits");

        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonHitsObject = jsonArray.get(i).getAsJsonObject();

            // 获取返回字段
            JsonObject sourceObject = jsonHitsObject.get("_source").getAsJsonObject();

            // 封装Article对象
            Article article = new Article();
            article.setTitle(sourceObject.get("title").getAsString());
            article.setContent(sourceObject.get("content").getAsString());
            article.setSource(sourceObject.get("source").getAsString());
            article.setAuthor(sourceObject.get("author").getAsString());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            article.setPubdate(sdf.parse(sourceObject.get("pubdate").getAsString()));
            article.setUrl(sourceObject.get("url").getAsString());

            // 获取高亮字段
            JsonObject highlightObject = jsonHitsObject.get("highlight").getAsJsonObject();
            String content = null;
            String title = null;
            String source = null;
            String author = null;
            if (highlightObject.get("content") != null) {
                content = highlightObject.get("content").getAsJsonArray().get(0).getAsString();
            }
            if (highlightObject.get("title") != null) {
                title = highlightObject.get("title").getAsJsonArray().get(0).getAsString();
            }
            if (highlightObject.get("source") != null) {
                source = highlightObject.get("source").getAsJsonArray().get(0).getAsString();
            }
            if (highlightObject.get("author") != null) {
                author = highlightObject.get("author").getAsJsonArray().get(0).getAsString();
            }
            if (content != null) {
                article.setContent(content);
            }
            if (title != null) {
                article.setTitle(title);
            }
            if (source != null) {
                article.setSource(source);
            }
            if (author != null) {
                article.setAuthor(author);
            }
            articles.add(article);
        }
    }
}
