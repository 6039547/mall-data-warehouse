package com.demo.service;

import com.demo.model.*;

import java.util.List;

public interface ActiveDataService {

    public List<ActiveTotalData> queryActiveTotal(String thisEntryDate);

    public List<ActiveData> queryActiveData(String tag,String thisEntryDate);

    public List<RetainData> queryRetainData();

    public List<GMVOrder> queryGMVOrder();

    public List<MapData> queryMapData();

    public ConvertData queryConvertData(String tag);
}
