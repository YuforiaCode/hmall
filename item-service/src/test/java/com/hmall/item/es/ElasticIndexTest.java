package com.hmall.item.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ElasticIndexTest {

    private RestHighLevelClient client;

    @Test
    void testConnection() {
        System.out.println("client = " + client);
    }

    /**
     * 创建索引库
     */
    @Test
    void testCreateIndex() throws IOException {
        //1.准备Request对象
        CreateIndexRequest request = new CreateIndexRequest("items");
        //2.准备请求参数，MAPPING_TEMPLATE是静态常量字符串，内容是JSON格式的Mapping映射参数
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求，client.indices()的返回值是IndicesClient类型，封装了所有与索引库操作有关的方法
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引库是否存在
     */
    @Test
    void testExistsIndex() throws IOException {
        //1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("items");
        //2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        //3.输出
        System.err.println(exists ? "索引库已经存在！" : "索引库不存在！");
    }

    /**
     * 删除索引库
     */
    @Test
    void testDeleteIndex() throws IOException {
        //1.创建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("items");
        //2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 初始化RestHighLevelClient
     */
    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.100.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /**
     * JSON格式的Mapping映射参数
     * 这里视频中说不需要库存，结果文档又说要库存，反正我是删掉了库存stock
     */
    private static final String MAPPING_TEMPLATE = "{\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"id\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"name\":{\n" +
            "        \"type\": \"text\",\n" +
            "        \"analyzer\": \"ik_max_word\"\n" +
            "      },\n" +
            "      \"price\":{\n" +
            "        \"type\": \"integer\"\n" +
            "      },\n" +
            "      \"image\":{\n" +
            "        \"type\": \"keyword\",\n" +
            "        \"index\": false\n" +
            "      },\n" +
            "      \"category\":{\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"brand\":{\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"sold\":{\n" +
            "        \"type\": \"integer\"\n" +
            "      },\n" +
            "      \"commentCount\":{\n" +
            "        \"type\": \"integer\",\n" +
            "        \"index\": false\n" +
            "      },\n" +
            "      \"isAD\":{\n" +
            "        \"type\": \"boolean\"\n" +
            "      },\n" +
            "      \"updateTime\":{\n" +
            "        \"type\": \"date\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
}
