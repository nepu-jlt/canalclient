
package com.ebei.canal.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.ebei.canal.common.Constants;
import com.ebei.canal.vo.RedisParams;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.text.ParseException;
import java.util.*;

public class RedisUtil {
    
    // Redis服务器IP
    private static String ADDRESS = ConfigUtils.getConfigValue("application.properties", "redis.address");
    
    // Redis的端口号
    private static int PORT = Integer.parseInt(ConfigUtils.getConfigValue("application.properties", "redis.port"));
    
    // 访问密码
    private static String AUTH = ConfigUtils.getConfigValue("application.properties", "redis.auth");
    
    private static String REDIS_EXPIRE = ConfigUtils.getConfigValue("application.properties", "redis.expire");
    
    // 可用连接实例的最大数目，默认值为8；
    // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_ACTIVE = 1024;
    
    // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 200;
    
    // 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = 10000;
    
    // 过期时间
    protected final static int EXPIRE_TIME = Integer.parseInt(ConfigUtils.getConfigValue("application.properties", "redis.expire"));
    
    // 连接池
    protected JedisPool pool;
    
    private static RedisUtil util = null;
    private RedisUtil() {}
    
    public static RedisUtil getInstance() {
        if (util == null) {
            synchronized (RedisUtil.class) {
                if (util == null) {
                    util = new RedisUtil();
                    util.buildPool();
                }
            }
        }
        return util;
    }
    
    public JedisPool buildPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(MAX_ACTIVE);
        //最多空闲实例
        config.setMaxIdle(MAX_IDLE);
        //超时时间
        config.setMaxWaitMillis(MAX_WAIT);
        //
        config.setTestOnBorrow(false);
        
        pool = new JedisPool(config, ADDRESS, PORT, 1000, AUTH);
        
        return pool;
    }
    
    /**
     * 获取jedis实例
     */
    protected synchronized Jedis getJedis() {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
        }
        return jedis;
    }
    
    /**
     * 释放jedis资源
     *
     * @param jedis
     * @param isBroken
     */
    protected void closeResource(Jedis jedis, boolean isBroken) {
        try {
            if (isBroken) {
                pool.returnBrokenResource(jedis);
            } else {
                pool.returnResource(jedis);
            }
        } catch (Exception e) {
        
        }
    }
    
    /**
     *  是否存在key
     *
     * @param key
     */
    public boolean existKey(String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            jedis.select(0);
            return jedis.exists(key);
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
        return false;
    }
    
    /**
     *  删除key
     *
     * @param key
     */
    public void delKey(String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            jedis.select(0);
            jedis.del(key);
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
    }
    
    /**
     *  取得key的值
     *
     * @param key
     */
    public String stringGet(String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        String lastVal = null;
        try {
            jedis = getJedis();
            jedis.select(0);
            lastVal = jedis.get(key);
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
        return lastVal;
    }
    
    /**
     *  添加string数据
     *
     * @param key
     * @param value
     */
    public String stringSet(String key, String value) {
        Jedis jedis = null;
        boolean isBroken = false;
        String lastVal = null;
        try {
            jedis = getJedis();
            jedis.select(0);
            lastVal = jedis.set(key, value);
            jedis.expire(key, EXPIRE_TIME);
        } catch (Exception e) {
            e.printStackTrace();
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
        return lastVal;
    }
    
    /**
     *  添加hash数据
     *
     * @param key
     * @param field
     * @param value
     */
    public void hashSet(String key, String field, String value) {
        boolean isBroken = false;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                jedis.select(0);
                jedis.hset(key, field, value);
                jedis.expire(key, EXPIRE_TIME);
            }
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
    }
    
    /**
     *  读取hash数据
     *
     * @param params
     */
    public Object read(RedisParams params) {
        JSONArray result = new JSONArray();
    
        boolean isBroken = false;
        Jedis jedis = null;
        Map<String, String> lastVal = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                jedis.select(0);
                if (StringUtils.isEmpty(params.getField())) {
                    lastVal = jedis.hgetAll((StringUtils.isEmpty(params.getNamespace())?"":(params.getNamespace() + ":")) + params.getKey());
                    if (lastVal != null) {
                        for (String key : lastVal.keySet()) {
                            result.add(JSONObject.parseObject(lastVal.get(key)));
                        }
                    }
                } else {
                    String hgetVal = jedis.hget((StringUtils.isEmpty(params.getNamespace())?"":(params.getNamespace() + ":")) + params.getKey(), params.getField());
                    if (hgetVal != null) {
                        result.add(JSONObject.parseObject(hgetVal));
                    }
                }
            }
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
            
        return result;
    }
    
    /**
     *  添加hash数据
     *
     * @param key
     * @param field
     */
    public Long hashDel(String key, String field) {
        Jedis jedis = null;
        boolean isBroken = false;
        Long reply = null;
        try {
            jedis = getJedis();
            jedis.select(0);
            reply = jedis.hdel(key, field);
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
        return reply;
    }
    
    public Long listLPush(String key, String value) {
        boolean isBroken = false;
        Jedis jedis = null;
        Long reply = null;
        try {
            jedis = getJedis();
            if (jedis != null) {
                jedis.select(0);
                reply = jedis.lpush(key, value);
                jedis.expire(key, EXPIRE_TIME);
            }
        } catch (Exception e) {
            isBroken = true;
        } finally {
            closeResource(jedis, isBroken);
        }
        return reply;
    }
    
    public void redisInsert(String database, String table, List<CanalEntry.Column> columns) {
        JSONObject json=new JSONObject();
        String field = UUID.randomUUID().toString();
        Date createTime = null;
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
            // 查找该表的主键列名配置
            String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + table);
            if (StringUtils.isEmpty(tableIdColumn)) {
                tableIdColumn = "id";
            }
            if (tableIdColumn.equalsIgnoreCase(column.getName())) {
                field = column.getValue();
            }
            // 如果是统计基础表，按照process_createtime判断,只插入最近n天数据(根据redis超时设置)
            if ("process_createtime".equals(column.getName())) {
                try {
                    createTime = DateUtils.parseDate(column.getValue(), new String[]{"yyyy-MM-dd HH:mm:ss"});
                } catch (ParseException e) {
                    System.out.println("获取process_createtime出错，process_createtime:" + column.getValue());
                }
            }
        }
        if(columns.size()>0){
//            RedisUtil.listLPush(database +":"+table, json.toJSONString());
            // 如果是统计基础表，只插入最近n天数据(根据redis超时设置，默认7天，7*24*3600*1000=604800000毫秒)
            if (StringUtils.isEmpty(REDIS_EXPIRE)) {
                REDIS_EXPIRE = "604800";
            }
            long redisExpire = Long.parseLong(REDIS_EXPIRE) * 1000;
            if (!"dynamicform_statistics".equals(table) || ( "dynamicform_statistics".equals(table) &&
                    createTime != null && ((new Date()).getTime() - createTime.getTime() <= redisExpire))) {
                this.hashSet(database + ":" + table, field, json.toJSONString());
            }
        }
    }
    
    public  void redisUpdate(String database, String table, List<CanalEntry.Column> columns){
        JSONObject json=new JSONObject();
        String field = UUID.randomUUID().toString();
        // 查找该表的主键列名配置
        String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + table);
        if (StringUtils.isEmpty(tableIdColumn)) {
            tableIdColumn = "id";
        }
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
            if (tableIdColumn.equalsIgnoreCase(column.getName())) {
                field = column.getValue();
            }
        }
        if(columns.size()>0){
//            RedisUtil.listLPush(database +":"+table, json.toJSONString());
            this.hashSet(database +":"+table, field, json.toJSONString());
        }
    }
    
    public  void redisDelete(String database, String table, List<CanalEntry.Column> columns){
        JSONObject json=new JSONObject();
        String field = UUID.randomUUID().toString();
        // 查找该表的主键列名配置
        String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + table);
        if (StringUtils.isEmpty(tableIdColumn)) {
            tableIdColumn = "id";
        }
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
            if (tableIdColumn.equalsIgnoreCase(column.getName())) {
                field = column.getValue();
            }
        }
        if(columns.size()>0){
//            RedisUtil.listLPush(database +":"+table, json.toJSONString());
            this.hashDel(database +":"+table, field);
        }
    }
    
    public void redisTruncate(String database, String table){
        this.delKey(database +":"+table);
    }
    
}