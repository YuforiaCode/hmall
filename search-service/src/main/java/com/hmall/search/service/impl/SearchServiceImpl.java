package com.hmall.search.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.search.domain.po.Item;
import com.hmall.search.domain.po.ItemDoc;
import com.hmall.search.domain.query.ItemPageQuery;
import com.hmall.search.domain.vo.CategoryAndBrandVo;
import com.hmall.search.mapper.SearchMapper;
import com.hmall.search.service.ISearchService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 商品表 服务实现类
 * </p>
 *
 * @author 虎哥
 */
@Service
public class SearchServiceImpl extends ServiceImpl<SearchMapper, Item> implements ISearchService {

    @Resource(name = "elasticsearchClient")
    private RestHighLevelClient client;

    /**
     * 基于ES搜索商品
     */
    public PageDTO<ItemDTO> EsSearch(ItemPageQuery query) {
        //1.准备request对象
        SearchRequest request = new SearchRequest("items");
        //精准总数
        request.source().trackTotalHits(true);

        //2.组织DSL参数
        //2.1.bool查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (StrUtil.isNotBlank(query.getKey())) {
            //关键词查询(按照相关性算分)
            boolQuery.must(QueryBuilders.matchQuery("name", query.getKey()));
        }
        if (StrUtil.isNotBlank(query.getCategory())) {
            //分类过滤
            boolQuery.filter(QueryBuilders.termQuery("category", query.getCategory()));
        }
        if (StrUtil.isNotBlank(query.getBrand())) {
            //品牌过滤
            boolQuery.filter(QueryBuilders.termQuery("brand", query.getBrand()));
        }
        if (query.getMaxPrice() != null) {
            //价格最大值过滤
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(query.getMaxPrice()));
        }
        if (query.getMinPrice() != null) {
            //价格最小值过滤
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(query.getMinPrice()));
        }
        //request.source().query(boolQuery);

        //2.2.追加算分条件(提升广告权重)
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(boolQuery,
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true),
                                ScoreFunctionBuilders.weightFactorFunction(100)
                        )
                }
        ).boostMode(CombineFunction.MULTIPLY);

        //2.3.设置提升广告权重后的查询
        request.source().query(functionScoreQuery);

        //2.4.分页条件
        request.source().from((query.getPageNo() - 1) * query.getPageSize()).size(query.getPageSize());

        //2.5.排序条件
        if (StrUtil.isNotBlank(query.getSortBy())) {
            //若指定排序字段，先按指定字段排序，再按分数排序(广告已提权)，最后按更新时间排序
            request.source().sort(query.getSortBy(), query.getIsAsc() ? SortOrder.ASC : SortOrder.DESC)
                    .sort("_score", SortOrder.DESC)
                    .sort("updateTime", query.getIsAsc() ? SortOrder.ASC : SortOrder.DESC);
        }else {
            //若未指定排序字段(默认排序)，先按分数排序(广告已提权)，再按更新时间排序
            request.source().sort("_score", SortOrder.DESC)
                    .sort("updateTime", query.getIsAsc() ? SortOrder.ASC : SortOrder.DESC);
        }

        //2.6.高亮条件
        request.source().highlighter(SearchSourceBuilder.highlight().field("name"));

        PageDTO<ItemDTO> result = null;
        try {
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.解析结果
            result = parseResponseResult(response, query);
        } catch (IOException e) {
            log.error("ES搜索失败，出现异常", e);
        }

        //5.返回结果
        return result;
    }

    /**
     * 获得分类和品牌的聚合值
     */
    public CategoryAndBrandVo getFilters(ItemPageQuery query) {
        //1.准备request对象
        SearchRequest request = new SearchRequest("items");

        //2.组织DSL参数
        //2.1.bool查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (StrUtil.isNotBlank(query.getKey())) {
            //关键词查询(按照相关性算分)
            boolQuery.must(QueryBuilders.matchQuery("name", query.getKey()));
        }
        if (StrUtil.isNotBlank(query.getCategory())) {
            //分类过滤
            boolQuery.filter(QueryBuilders.termQuery("category", query.getCategory()));
        }
        if (StrUtil.isNotBlank(query.getBrand())) {
            //品牌过滤
            boolQuery.filter(QueryBuilders.termQuery("brand", query.getBrand()));
        }
        if (query.getMaxPrice() != null) {
            //价格最大值过滤
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(query.getMaxPrice()));
        }
        if (query.getMinPrice() != null) {
            //价格最小值过滤
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(query.getMinPrice()));
        }
        request.source().query(boolQuery);

        //2.2.分页
        request.source().size(0);

        //2.3.聚合条件
        String brandAggName = "brand_agg";
        String categoryAggName = "category_agg";
        request.source().aggregation(
                AggregationBuilders.terms(brandAggName).field("brand").size(10)
        );
        request.source().aggregation(
                AggregationBuilders.terms(categoryAggName).field("category").size(10)
        );
        List<String> brandList = null;
        List<String> categoryList = null;

        try {
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.解析结果
            Aggregations aggregations = response.getAggregations();
            //4.1.根据聚合名称获取对应的聚合
            Terms brandTerms = aggregations.get(brandAggName);
            Terms categoryTerms = aggregations.get(categoryAggName);
            //4.2.获取buckets
            List<? extends Terms.Bucket> brandBuckets = brandTerms.getBuckets();
            List<? extends Terms.Bucket> categoryBuckets = categoryTerms.getBuckets();
            //4.3.遍历获取每一个bucket
            brandList = new ArrayList<>();
            categoryList = new ArrayList<>();
            for (Terms.Bucket brandBucket : brandBuckets) {
                brandList.add(brandBucket.getKeyAsString());
            }
            for (Terms.Bucket categoryBucket : categoryBuckets) {
                categoryList.add(categoryBucket.getKeyAsString());
            }
        } catch (IOException e) {
            log.error("ES聚合失败，出现异常", e);
        }

        //5.返回结果
        CategoryAndBrandVo categoryAndBrandVo = new CategoryAndBrandVo();
        categoryAndBrandVo.setCategory(categoryList);
        categoryAndBrandVo.setBrand(brandList);
        return categoryAndBrandVo;
    }

    /**
     * 封装解析结果的API
     */
    private PageDTO<ItemDTO> parseResponseResult(SearchResponse response, ItemPageQuery query) {
        //1.创建一个集合
        List<ItemDoc> itemDocList = new ArrayList<>();

        SearchHits searchHits = response.getHits();
        //2.1.总条数
        long total = searchHits.getTotalHits().value;
        //2.2.命中的数据
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            //2.2.1.获取source结果
            String json = hit.getSourceAsString();
            //2.2.2.转为ItemDoc
            ItemDoc itemDoc = JSONUtil.toBean(json, ItemDoc.class);
            //2.3.处理高亮结果
            Map<String, HighlightField> hfs = hit.getHighlightFields();
            if (hfs != null && !hfs.isEmpty()) {
                //2.3.1.根据高亮字段名获取高亮结果
                HighlightField hf = hfs.get("name");
                //2.3.2.获取高亮结果，覆盖非高亮结果
                String hfName = hf.getFragments()[0].string();
                itemDoc.setName(hfName);
            }
            //2.4.添加到集合中
            itemDocList.add(itemDoc);
        }

        //3.1.转为ItemDTO集合
        List<ItemDTO> itemDTOList = BeanUtil.copyToList(itemDocList, ItemDTO.class);
        //3.2.页面大小
        Long pageSize = query.getPageSize().longValue();
        //3.3.返回结果
        return new PageDTO<>(total, pageSize, itemDTOList);
    }
}
