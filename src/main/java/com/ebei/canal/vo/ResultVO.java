package com.ebei.canal.vo;

import lombok.Data;

@Data
public class ResultVO<T> {
    
    private String status;
    
    private String message;
    
    private T data;
    
    public ResultVO(String status, String message) {
        this.status = status;
        this.message = message;
    }
    
    public ResultVO(String status, String message, T data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }
}
