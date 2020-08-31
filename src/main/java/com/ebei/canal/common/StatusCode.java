package com.ebei.canal.common;

public class StatusCode {
    
    /**
     * 成功
     */
    public static final String SUCCESS = "10200";
    
    /**
     * Token失效，或者未传Token，或者没有权限
     */
    public static final String UNAUTHORIATION = "10401";
    
    /**
     * 内部服务器错误
     */
    public static final String ERROR = "10500";
    
    /**
     * 	业务错误 例 : 参数不匹配或参数验证不正确
     */
    public static final String BUSINESS_ERROR = "10400";
    
    /**
     * 无数据
     */
    public static final String NO_DATA = "10204";
}
