package com.hmall.item.es;

import cn.hutool.json.JSONUtil;
import com.hmall.item.domain.po.ItemDoc;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

//@SpringBootTest(properties = {"spring.profiles.active=local", "seata.enabled=false"})
public class ElasticSearchTest {

    private RestHighLevelClient client;

    /**
     * MatchAll查询所有(JavaRestClient快速入门)
     */
    @Test
    void testMatchAll() throws IOException {
        //1.创建request对象
        SearchRequest request = new SearchRequest("items");
        //2.配置request参数
        request.source()
                .query(QueryBuilders.matchAllQuery());  //QueryBuilders是工具类
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.解析结果
        parseResponseResult(response);
    }

    /**
     * 构建复杂查询条件的搜索
     */
    @Test
    void testSearch() throws IOException {
        //1.创建request对象
        SearchRequest request = new SearchRequest("items");
        //2.组织DSL参数
        request.source().query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("name", "脱脂牛奶"))
                        .filter(QueryBuilders.termQuery("brand", "德亚"))
                        .filter(QueryBuilders.rangeQuery("price").lt(10000))
        );
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.解析结果
        parseResponseResult(response);
    }

    /**
     * 排序和分页
     */
    @Test
    void testSortAndPage() throws IOException {
        //0.模拟前端传递的分页参数
        int pageNo = 2, pageSize = 5;

        //1.创建request对象
        SearchRequest request = new SearchRequest("items");
        //2.组织DSL参数
        //2.1.query条件
        request.source().query(QueryBuilders.matchAllQuery());
        //2.2.分页
        request.source().from((pageNo - 1) * pageSize).size(pageSize);
        //2.3.排序
        request.source()
                .sort("sold", SortOrder.DESC)
                .sort("price", SortOrder.ASC);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //4.解析结果
        parseResponseResult(response);
    }

    /**
     * 封装解析结果的api代码
     */
    private static void parseResponseResult(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        //4.1.总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("total = " + total);
        //4.2.命中的数据
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            //4.2.1.获取source结果
            String json = hit.getSourceAsString();
            //4.2.2.转为itemDoc
            ItemDoc itemDoc = JSONUtil.toBean(json, ItemDoc.class);

            System.out.println("itemDoc = " + itemDoc);
        }
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
