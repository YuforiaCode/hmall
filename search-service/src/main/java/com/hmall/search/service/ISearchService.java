package com.hmall.search.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.search.domain.po.Item;
import com.hmall.search.domain.query.ItemPageQuery;

import java.io.IOException;

/**
 * <p>
 * 商品表 服务类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
public interface ISearchService extends IService<Item> {

    /**
     * 基于ES搜索商品
     */
    PageDTO<ItemDTO> EsSearch(ItemPageQuery query) throws IOException;
}
