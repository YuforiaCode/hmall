package com.hmall.api.client;

import com.hmall.api.client.fallback.ItemClientFallbackFactory;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;

@FeignClient(value = "item-service", fallbackFactory = ItemClientFallbackFactory.class)  //声明服务的名字
public interface ItemClient {
    /**
     * 根据id批量查询商品
     */
    @GetMapping("/items")  //请求方式、请求路径
    List<ItemDTO> queryItemByIds(@RequestParam("ids") Collection<Long> ids);  //返回值类型、请求参数

    /**
     * 根据id查询商品
     */
    @GetMapping("{id}")
    ItemDTO queryItemById(@PathVariable("id") Long id);

    /**
     * 批量扣减库存
     */
    @PutMapping("/items/stock/deduct")
    void deductStock(@RequestBody List<OrderDetailDTO> items);

    /**
     * 批量恢复库存
     */
    @PutMapping("/items/stock/restore")
    void restoreStock(@RequestBody List<OrderDetailDTO> items);
}
