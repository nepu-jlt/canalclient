package com.ebei.canal.controller;

import com.ebei.canal.common.StatusCode;
import com.ebei.canal.common.StatusCodeDesc;
import com.ebei.canal.util.RedisUtil;
import com.ebei.canal.vo.RedisParams;
import com.ebei.canal.vo.ResultVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Cache Controller
 *
 * @author Victor.You@ebeitech
 * @version 1.0
 * @created 2018/6/1
 */

@RestController
@RequestMapping("/redis")
public class RedisController {
    
    private RedisUtil redisUtil;
    
    @PostMapping("/read")
    public ResultVO<Object> read(@RequestBody RedisParams redisParams) {
        redisUtil = RedisUtil.getInstance();
        Object value = redisUtil.read(redisParams);
        if (value != null) {
            return new ResultVO<>(StatusCode.SUCCESS, StatusCodeDesc.SUCCESS_DESC, value);
        }
        return new ResultVO<>(StatusCode.NO_DATA, StatusCodeDesc.SUCCESS_DESC);
    }
    
}
