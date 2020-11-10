package com.demo.mapper;

import com.demo.model.Index;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;


@Mapper
public interface IndexMapper {

    List<Index> findAll();
}
