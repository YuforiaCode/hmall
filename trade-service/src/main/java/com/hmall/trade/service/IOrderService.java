package com.hmall.trade.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.trade.domian.dto.OrderFormDTO;
import com.hmall.trade.domian.po.Order;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
public interface IOrderService extends IService<Order> {

    Long createOrder(OrderFormDTO orderFormDTO);

    /**
     * 延迟信息修改订单状态为已支付
     */
    void markOrderPaySuccess(Long orderId);

    /**
     * 延迟信息取消订单(修改订单状态为已关闭及恢复库存)
     */
    void cancelOrder(Long orderId);
}
