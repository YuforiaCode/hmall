package com.hmall.item.es;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.po.ItemDoc;
import com.hmall.item.service.IItemService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest(properties = {"spring.profiles.active=local", "seata.enabled=false"})
public class ElasticDocumentTest {

    private RestHighLevelClient client;
    @Autowired
    private IItemService itemService;

    /**
     * 新增文档(id存在时就是修改文档的全量更新)
     */
    @Test
    void testIndexDocument() throws IOException {
        //0.准备文档数据
        //0.1.根据id查询数据库数据
        Item item = itemService.getById(100000011127L);
        //0.2.将数据库数据转为文档数据
        ItemDoc itemDoc = BeanUtil.copyProperties(item, ItemDoc.class);

        itemDoc.setPrice(29900);

        //1.准备request
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        //2.准备请求参数
        request.source(JSONUtil.toJsonStr(itemDoc), XContentType.JSON);
        //3.发送请求
        IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
        System.out.println("resp = " + resp);
    }

    /**
     * 查询文档
     */
    @Test
    void testGetDocument() throws IOException {
        //1.准备request
        GetRequest request = new GetRequest("items", "100000011127");
        //2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.解析响应中的source
        String json = response.getSourceAsString();
        ItemDoc doc = JSONUtil.toBean(json, ItemDoc.class);
        System.out.println("doc = " + doc);
    }

    /**
     * 删除文档
     */
    @Test
    void testDeleteDocument() throws IOException {
        //1.准备request
        DeleteRequest request = new DeleteRequest("items", "100000011127");
        //2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 修改文档(局部更新)
     */
    @Test
    void testUpdateDocument() throws IOException {
        //1.准备request
        UpdateRequest request = new UpdateRequest("items", "100000011127");
        //2.准备请求参数
        request.doc(
                "price", 25600
        );
        //3.发送请求
        client.update(request, RequestOptions.DEFAULT);
    }

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
}
