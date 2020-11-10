package com.demo.service.impl;

import com.demo.mapper.IndexMapper;
import com.demo.model.Index;
import com.demo.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zhouyulong
 * @date 2018/10/28 下午2:46.
 */
@Service
public class IndexServiceImpl implements IndexService {
    @Autowired
    private IndexMapper indexMapper;

    @Override
    public List<Index> findAll() {
        return indexMapper.findAll();
    }

}
