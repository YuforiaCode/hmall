package com.hmall.api.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Collection;

@FeignClient("cart-service")
public interface CartClient {
    /**
     * 批量删除购物车
     */
    @DeleteMapping("carts")
    void deleteCartItemByIds(@RequestParam("ids") Collection<Long> ids);
}
