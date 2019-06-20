package com.daima;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EsTest {

    @Test
    //创建索引+插入文档
    public void fun01() throws Exception{
        //1.创建客户端连接对象，设置连接地址
        TransportClient client =new PreBuiltTransportClient(Settings.EMPTY);

        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //2.创建文档数据
        Map<String,Object> map = new HashMap<String, Object>();
        map.put("id",1);
        map.put("title","ElasticSearch是一个基于Lucene的搜索服务器");
        map.put("content","ElasticSearch是一个基于Lucene的" +
                "搜索服务器。它提供了一个分布式多用户能力的全文" +
                "搜索引擎，基于RESTful web接口。" +
                "Elasticsearch是用Java开发的，" +
                "并作为Apache许可条款下的开放源码发布，" +
                "是当前流行的企业级搜索引擎。设计用于云计算中，" +
                "能够达到实时搜索，稳定，可靠，快速，安装使用方便");


        //3.创建索引，创建文档类型，设置唯一主键，设置文档
        client.prepareIndex("blog","article","1").setSource(map).get();

        //4.释放资源
        client.close();
    }


    //查询所有文档
    @Test
    public void fun02()throws Exception{
        //1.创建客户端连接对象，设置连接地址
        PreBuiltTransportClient client = new PreBuiltTransportClient(Settings.EMPTY);

        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //2.指定索引库和类型
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog").setTypes("article");

        //3.设置查询条件，进行查询
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).get();

        //4.处理结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("条数"+hits.totalHits);

        for (SearchHit hit : hits) {
            System.out.println("id"+hit.getId());
            System.out.println("标题="+hit.getSourceAsMap().get("title"));
            System.out.println("内容="+hit.getSourceAsMap().get("content"));
            System.out.println("---------------");
        }
    }

    //字符串查询
    @Test
    public void test3()throws Exception{
        //创建客户端访问对象
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        //设置查询条件
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article").setQuery(QueryBuilders.queryStringQuery("搜索").field("title")).get();
        //默认在所有的字段上进行搜索，搜索“搜索” ，如果添加.field("title"):表示只在title字段进行搜索

        //处理结果
        SearchHits hits = searchResponse.getHits(); //获得命中目标，即查询了多少个对象

        System.out.println("共查询"+hits.getTotalHits()+"条");

        Iterator<SearchHit> ite = hits.iterator();

        while (ite.hasNext()) {
            SearchHit searchHit = ite.next();

            System.out.println(searchHit.getSourceAsString());

            System.out.println(searchHit.getSourceAsMap().get("title"));
        }

        //关闭资源
        client.close();
    }
}
