package com.hmall.trade.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmall.api.client.CartClient;
import com.hmall.api.client.ItemClient;
import com.hmall.api.client.PayClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.utils.BeanUtils;
import com.hmall.common.utils.UserContext;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domian.dto.OrderFormDTO;
import com.hmall.trade.domian.po.Order;
import com.hmall.trade.domian.po.OrderDetail;
import com.hmall.trade.mapper.OrderMapper;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import io.seata.spring.annotation.GlobalTransactional;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
@Service
@RequiredArgsConstructor
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order> implements IOrderService {

    private final ItemClient itemClient;
    private final IOrderDetailService detailService;
    private final CartClient cartClient;
    private final RabbitTemplate rabbitTemplate;
    private final PayClient payClient;

    @Override
    @GlobalTransactional
    public Long createOrder(OrderFormDTO orderFormDTO) {
        // 1.订单数据
        Order order = new Order();
        // 1.1.查询商品
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        // 1.2.获取商品id和数量的Map
        Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();
        // 1.3.查询商品
        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("商品不存在");
        }
        // 1.4.基于商品价格、购买数量计算商品总价：totalFee
        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }
        order.setTotalFee(total);
        // 1.5.其它属性
        order.setPaymentType(orderFormDTO.getPaymentType());
        order.setUserId(UserContext.getUser());
        order.setStatus(1);
        // 1.6.将Order写入数据库order表中
        save(order);

        // 2.保存订单详情
        List<OrderDetail> details = buildDetails(order.getId(), items, itemNumMap);
        detailService.saveBatch(details);

        // 3.清理购物车商品
        cartClient.deleteCartItemByIds(itemIds);

        // 4.扣减库存
        try {
            itemClient.deductStock(detailDTOS);
        } catch (Exception e) {
            throw new RuntimeException("库存不足！");
        }

        // 5.发送延迟消息，检测订单支付状态
        rabbitTemplate.convertAndSend(
                MQConstants.DELAY_EXCHANGE_NAME,
                MQConstants.DELAY_ORDER_KEY,
                order.getId(),
                message -> {
                    message.getMessageProperties().setDelay(1800000); //实际和前端一致设置延迟时间为30分钟，测试时为10秒
                    return message;
                });

        return order.getId();
    }

    /**
     * 延迟信息修改订单状态为已支付
     * 前提：延迟信息发现订单存在但是订单状态未支付，同时查询订单流水状态为支付成功
     * 兜底：为订单支付成功但是没有修改订单状态的错误场景兜底(网络中断等情况导致发送支付状态通知失败)
     * 关联逻辑：为PayOrderServiceImpl中的tryPayOrderByBalance方法中的5.修改订单状态(发送支付状态通知)兜底
     *          在OrderDelayMessageListener中的listenOrderDelayMessage方法中调用
     */
    public void markOrderPaySuccess(Long orderId) {
        Order order = new Order();
        order.setId(orderId);
        order.setStatus(2);
        order.setPayTime(LocalDateTime.now());
        updateById(order);
    }

    /**
     * 延迟信息取消订单(修改订单状态为已关闭及恢复库存)
     * 前提：延迟信息发现订单存在但是订单状态未支付，同时查询订单流水状态为支付超时或取消
     * 兜底：为订单支付超时或者取消订单但是没有修改订单状态和恢复库存的错误场景兜底(网络中断等情况导致发送取消订单通知失败)
     * 关联逻辑：为PayOrderServiceImpl中的tryPayOrderByBalance方法中的5.修改订单状态(发送支付状态通知)兜底
     *          在OrderDelayMessageListener中的listenOrderDelayMessage方法中调用
     */
    public void cancelOrder(Long orderId) {
        //1.修改订单状态为已关闭
        Order order = new Order();
        order.setId(orderId);
        order.setStatus(5);
        order.setPayTime(LocalDateTime.now());
        updateById(order);
        //2.修改支付流水状态为已取消
        payClient.updatePayOrderStatusByBizOrderNo(orderId, 2);
        //3.恢复库存
        //3.1.获取orderDetail的list
        List<OrderDetail> details = detailService.lambdaQuery().eq(OrderDetail::getOrderId, orderId).list();
        //3.2.构建orderDetailDTO的list
        List<OrderDetailDTO> dtos = BeanUtils.copyList(details, OrderDetailDTO.class);
        itemClient.restoreStock(dtos);
    }

    private List<OrderDetail> buildDetails(Long orderId, List<ItemDTO> items, Map<Long, Integer> numMap) {
        List<OrderDetail> details = new ArrayList<>(items.size());
        for (ItemDTO item : items) {
            OrderDetail detail = new OrderDetail();
            detail.setName(item.getName());
            detail.setSpec(item.getSpec());
            detail.setPrice(item.getPrice());
            detail.setNum(numMap.get(item.getId()));
            detail.setItemId(item.getId());
            detail.setImage(item.getImage());
            detail.setOrderId(orderId);
            details.add(detail);
        }
        return details;
    }
}
