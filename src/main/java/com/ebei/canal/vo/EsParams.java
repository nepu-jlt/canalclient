package com.ebei.canal.vo;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class EsParams {
    
    private Integer queryType; // 查询类型：不传或1：按照id查询单条；2：多字段分字段匹配和多字段整句匹配
    
    private String database;
    
    private String table;
    
    private Integer pageSize;
    
    private Integer pageIndex;
    
    private JSONObject params;
    // queryType=1:传{"id":"value"};
    // queryType=2:传
    // {"key":"value","key2":"value2",
    // "keys":["key1"..."keyn"],
    // "keywords":["keyword1"..."keywordm"],
    // "start_date": {"type":"timeRange","start":"2019-12-01 00:00:00","end":"2019-12-31 23:59:59"},
    // "state":{"type":"countRange","start":"1","end":"7"} 类似为1-7这样的值
    // project_id":{"type":"findIn","value":["id1","id2",..."idn"]}}

}
