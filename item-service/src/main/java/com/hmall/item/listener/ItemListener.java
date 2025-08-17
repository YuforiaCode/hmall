package com.hmall.item.listener;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.item.domain.po.ItemDoc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.hmall.item.constants.MQConstants.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class ItemListener {

    private final ItemClient itemClient;

    private final RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                    HttpHost.create("http://192.168.100.128:9200"))
    );

    /**
     * 监听新增商品  新增  index
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = ITEM_INDEX_SAVE_QUEUE_NAME, durable = "true"),
            exchange = @Exchange(name = ITEM_EXCHANGE_NAME, type = ExchangeTypes.DIRECT),
            key = ITEM_INDEX_SAVE_KEY
    ))
    public void listenSaveItem(Long id) throws IOException {
        //读取数据库对象
        ItemDTO itemDTO = itemClient.queryItemById(id);
        if (itemDTO == null) {
            return;
        }
        //转换
        ItemDoc itemDoc = BeanUtil.copyProperties(itemDTO, ItemDoc.class);
        //Json化
        String jsonStr = JSONUtil.toJsonStr(itemDoc);
        //准备request对象
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        //准备参数
        request.source(jsonStr, XContentType.JSON);
        //发送请求
        restHighLevelClient.index(request, RequestOptions.DEFAULT);

        log.info("监听到RabbitMQ发送的新增商品的通知信息，开始同步更新索引库的数据");
    }

    /**
     * 监听更新商品状态  局部更新  update
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = ITEM_UPDATE_STATUS_QUEUE_NAME, durable = "true"),
            exchange = @Exchange(name = ITEM_EXCHANGE_NAME, type = ExchangeTypes.DIRECT),
            key = ITEM_UPDATE_STATUS_KEY
    ))
    public void listenUpdateItemStatus(Long id) throws IOException {
        //读取数据库对象
        ItemDTO itemDTO = itemClient.queryItemById(id);
        if (itemDTO == null) {
            return;
        }
        //转换
        ItemDoc itemDoc = BeanUtil.copyProperties(itemDTO, ItemDoc.class);
        //Json化
        String jsonStr = JSONUtil.toJsonStr(itemDoc);
        //准备request对象
        UpdateRequest request = new UpdateRequest("items", itemDoc.getId());
        //准备参数
        request.doc(jsonStr, XContentType.JSON);
        //发送请求
        restHighLevelClient.update(request, RequestOptions.DEFAULT);

        log.info("监听到RabbitMQ发送的局部更新商品状态的通知信息，开始同步更新索引库的数据");
    }

    /**
     * 监听更新商品  全局更新  index
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = ITEM_INDEX_UPDATE_QUEUE_NAME, durable = "true"),
            exchange = @Exchange(name = ITEM_EXCHANGE_NAME, type = ExchangeTypes.DIRECT),
            key = ITEM_INDEX_UPDATE_KEY
    ))
    public void listenUpdateItem(Long id) throws IOException {
        //读取数据库对象
        ItemDTO itemDTO = itemClient.queryItemById(id);
        if (itemDTO == null) {
            return;
        }
        //转换
        ItemDoc itemDoc = BeanUtil.copyProperties(itemDTO, ItemDoc.class);
        //Json化
        String jsonStr = JSONUtil.toJsonStr(itemDoc);
        //准备request对象
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        //准备参数
        request.source(jsonStr, XContentType.JSON);
        //发送请求
        restHighLevelClient.index(request, RequestOptions.DEFAULT);

        log.info("监听到RabbitMQ发送的全局更新商品的通知信息，开始同步更新索引库的数据");
    }

    /**
     * 监听删除商品  delete
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = ITEM_DELETE_QUEUE_NAME, durable = "true"),
            exchange = @Exchange(name = ITEM_EXCHANGE_NAME, type = ExchangeTypes.DIRECT),
            key = ITEM_DELETE_KEY
    ))
    public void listenDeleteItem(Long id) throws IOException {
        //准备request对象
        DeleteRequest request = new DeleteRequest("items", id.toString());
        //发送请求
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);

        log.info("监听到RabbitMQ发送的删除商品的通知信息，开始同步更新索引库的数据");
    }
}
