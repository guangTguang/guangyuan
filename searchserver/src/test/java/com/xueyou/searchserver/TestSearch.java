package com.xueyou.searchserver;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearch {

    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    @Test
    public void searchALL() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        searchSourceBuilder.query(QueryBuilders.termQuery("name","spring"));
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
        }

    }

    @Test
    public void searchTerms() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        searchSourceBuilder.query(QueryBuilders.termsQuery("_id",new String[]{"1","2"}));
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
        }
        //设置搜索源

        //搜索结果匹配

        //匹配度较高的前n个文档
        //日期格式化对象
    }

    //metchquery
    @Test
    public void searchMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发框架").minimumShouldMatch("80%"));
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();


        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }
    }

    //Mutilmetchquery
    @Test
    public void searchMutilMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("spring 开发","name" ,"description").minimumShouldMatch("50%").field("name",10));
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }
    }

    //booleanQuery
    @Test
    public void searchBooleanMutilMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring 开发", "name", "description").minimumShouldMatch("50%").field("name", 10);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(termQueryBuilder);
        boolQueryBuilder.must(multiMatchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }
    }

    //filter
    @Test
    public void searchBooleanFilterMutilMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring 开发", "name", "description").minimumShouldMatch("50%").field("name", 10);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel","201001"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(80).lte(100));
        boolQueryBuilder.must(multiMatchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }
    }

    //sort
    @Test
    public void searchSortMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //搜索方式

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(100));

        searchSourceBuilder.sort("studymodel",SortOrder.DESC);
        searchSourceBuilder.sort("price",SortOrder.ASC);
        searchSourceBuilder.query(boolQueryBuilder);
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }

    }

    //HighLightBuilder
    @Test
    public void searchHighLightBuilderMetch() throws IOException, ParseException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("xc_course");
        //设置类型
        searchRequest.types("doc");
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        //搜索方式

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发框架", "name").minimumShouldMatch("50%").field("name", 10);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(100));
        boolQueryBuilder.must(multiMatchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        //搜索源过滤字段
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp","description"},new String[]{});

        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>");
        highlightBuilder.postTags("</tag>");
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));

        searchSourceBuilder.highlighter(highlightBuilder);

        //页码
        int page =1;
        //每页记录数
        int size = 1;
        int from = (page-1)*size;
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        //向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse searchResponse = client.search(searchRequest);

        //搜索结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.getTotalHits();
        //得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

        for (SearchHit hit:searchHits) {
            //文档主键
            hit.getId();
            //原文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            StringBuffer stringBuffer = new StringBuffer();
            if (highlightFields!=null){
                HighlightField namehighlightField = highlightFields.get("name");
                if (namehighlightField!=null){
                    Text[] texts = namehighlightField.getFragments();
                    for (Text text:texts) {
                        stringBuffer.append(text);
                    }
                    name = stringBuffer.toString();
                }
            }
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            String timestamp = (String) sourceAsMap.get("timestamp");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(price);
            System.out.println(timestamp);
            System.out.println(description);
        }
    }
}
