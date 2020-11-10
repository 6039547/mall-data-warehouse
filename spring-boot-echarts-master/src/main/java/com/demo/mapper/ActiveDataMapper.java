package com.demo.mapper;

import com.demo.model.*;
import org.apache.ibatis.annotations.Mapper;

import java.util.HashMap;
import java.util.List;

@Mapper
public interface ActiveDataMapper {

    public ActiveTotalData queryDayCount(HashMap<String,String> map);
    public List<ActiveData> queryDay(HashMap<String,String> map);


    public ActiveTotalData queryWkCount(HashMap<String,String> map);
    public List<ActiveData> queryWk(HashMap<String,String> map);

    public ActiveTotalData queryMonCount(HashMap<String,String> map);
    public List<ActiveData> queryMon(HashMap<String,String> map);

    public List<RetainData> queryRetainData();

    public List<GMVOrder> queryGMVOrder();

    public List<MapData> queryMapData();

    public ConvertData queryConvertData(String tag);
}
