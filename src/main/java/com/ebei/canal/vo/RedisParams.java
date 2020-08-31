package com.ebei.canal.vo;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class RedisParams {
    
    private String namespace;
    
    private String key;
    
    private String field; // only for Hash
    
    private JSONObject value;
    
    private Integer type; // 数据类型：0-String，1-Hash，2-List，3-Set，4-ZSet
    
    private Integer score; // only for ZSet
    
}
