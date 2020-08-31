package com.ebei.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.ebei.canal.util.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class DataProcessor {
    protected final static Logger logger = LoggerFactory.getLogger(CanalClient.class);
    
    private static String DATABASE = ConfigUtils.getConfigValue("application.properties", "canal.database");
    
    private static String TABLE = ConfigUtils.getConfigValue("application.properties", "canal.table");
    
    private static String OPERATOR = ConfigUtils.getConfigValue("application.properties", "canal.operator");
    
    private static String CANAL_OUTPUT = ConfigUtils.getConfigValue("application.properties", "canal.output");
    
    private static ElasticSearchUtil esUtil;
    private static RedisUtil redisUtil;
    private static MySQLUtil mySQLUtil;
    private static OracleUtil oracleUtil;
    
    public static void process(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
            
            CanalEntry.EventType eventType = rowChage.getEventType();
//            logger.info(++processCount + String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
//                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
//                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
//                    eventType));
            if (eventType == CanalEntry.EventType.TRUNCATE && OPERATOR.contains("TRUNCATE")) {
                if (StringUtils.isEmpty(DATABASE) ||
                        (entry.getHeader().getSchemaName()!=null && isContain(DATABASE.split(","),entry.getHeader().getSchemaName()))) {
                    if (StringUtils.isEmpty(TABLE) ||
                            (entry.getHeader().getTableName() != null && isContain(TABLE.split(","), entry.getHeader().getTableName()))) {
                        logger.info("TRUNCATE TABLE " + entry.getHeader().getTableName());
                        if (CANAL_OUTPUT.contains("redis")) {
                            redisUtil = RedisUtil.getInstance();
                            redisUtil.redisTruncate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                        }
                        if (CANAL_OUTPUT.contains("elasticsearch")) {
                            esUtil = ElasticSearchUtil.getInstance();
                            esUtil.esTruncate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                        }
                        if (CANAL_OUTPUT.contains("mysql")) {
                            mySQLUtil = MySQLUtil.getInstance();
                            try {
                                mySQLUtil.mySQLTruncate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                            } catch (SQLException e) {
                                e.printStackTrace();
                                logger.error("MySQL执行同步truncate出错,dataBase:" + entry.getHeader().getSchemaName() + ",table:" + entry.getHeader().getTableName());
                            }
                        }
                        if (CANAL_OUTPUT.contains("oracle")) {
                            oracleUtil = OracleUtil.getInstance();
                            try {
                                oracleUtil.oracleTruncate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                            } catch (SQLException e) {
                                e.printStackTrace();
                                logger.error("Oracle执行同步truncate出错,dataBase:" + entry.getHeader().getSchemaName() + ",table:" + entry.getHeader().getTableName());
                            }
                        }
                    }
                }
            }
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                // 过滤database, table, operator
                if (StringUtils.isEmpty(DATABASE) ||
                        (entry.getHeader().getSchemaName()!=null && isContain(DATABASE.split(","),entry.getHeader().getSchemaName()))) {
                    if (StringUtils.isEmpty(TABLE) ||
                            (entry.getHeader().getTableName()!=null && isContain(TABLE.split(","),entry.getHeader().getTableName()))) {
                        // ES or redis or MySQL
                        if (CANAL_OUTPUT.contains("redis")) {
                            redisUtil = RedisUtil.getInstance();
                            if (eventType == CanalEntry.EventType.DELETE && OPERATOR.contains("DELETE")) {
                                redisUtil.redisDelete(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
                            } else if (eventType == CanalEntry.EventType.INSERT && OPERATOR.contains("INSERT")) {
                                redisUtil.redisInsert(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                            } else if (eventType == CanalEntry.EventType.UPDATE && OPERATOR.contains("UPDATE")) {
//                            logger.info("-------> before");
//                            printColumn(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
//                            logger.info("-------> after");
                                redisUtil.redisUpdate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                            } else {
                                // nothing to do
                            }
                        }
                        if (CANAL_OUTPUT.contains("elasticsearch")) {
                            esUtil = ElasticSearchUtil.getInstance();
                            if (eventType == CanalEntry.EventType.DELETE && OPERATOR.contains("DELETE")) {
                                esUtil.esDelete(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
                            } else if (eventType == CanalEntry.EventType.INSERT && OPERATOR.contains("INSERT")) {
                                esUtil.esInsert(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                            } else if (eventType == CanalEntry.EventType.UPDATE && OPERATOR.contains("UPDATE")) {
//                            logger.info("-------> before");
//                            printColumn(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
//                            logger.info("-------> after");
                                esUtil.esUpdate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                            } else {
                                // nothing to do
                            }
                        }
                        if (CANAL_OUTPUT.contains("mysql")) {
                            mySQLUtil = MySQLUtil.getInstance();
                            try {
                                if (eventType == CanalEntry.EventType.DELETE && OPERATOR.contains("DELETE")) {
                                    mySQLUtil.mySQLDelete(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
                                } else if (eventType == CanalEntry.EventType.INSERT && OPERATOR.contains("INSERT")) {
                                    mySQLUtil.mySQLInsert(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                                } else if (eventType == CanalEntry.EventType.UPDATE && OPERATOR.contains("UPDATE")) {
                                    mySQLUtil.mySQLUpdate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                                } else {
                                    // nothing to do
                                }
                            } catch (SQLException e) {
                                logger.error("MySQL执行同步" + eventType + "出错,dataBase:"+entry.getHeader().getSchemaName()+",table:"+entry.getHeader().getTableName(), e);
                            }
                        }
                        if (CANAL_OUTPUT.contains("oracle")) {
                            oracleUtil = OracleUtil.getInstance();
                            try {
                                if (eventType == CanalEntry.EventType.DELETE && OPERATOR.contains("DELETE")) {
                                    oracleUtil.oracleDelete(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList());
                                } else if (eventType == CanalEntry.EventType.INSERT && OPERATOR.contains("INSERT")) {
                                    oracleUtil.oracleInsert(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getAfterColumnsList());
                                } else if (eventType == CanalEntry.EventType.UPDATE && OPERATOR.contains("UPDATE")) {
                                    oracleUtil.oracleUpdate(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                                } else {
                                    // nothing to do
                                }
                            } catch (SQLException e) {
                                logger.error("Oracle执行同步" + eventType + "出错,dataBase:"+entry.getHeader().getSchemaName()+",table:"+entry.getHeader().getTableName(), e);
                            }
                        }
                    }
                }
            }
        }
    }
    
    public static boolean isContain(String[] list, String value) {
        if (list == null || value == null) return false;
        for (String lv : list) {
            if (value.trim().equals(lv.trim())) {
                return true;
            }
        }
        return false;
    }
    
    private static void printColumn(String database, String table, List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            logger.info(database + "-" + table + "-" + column.getName() + " : " + column.getValue() + " update=" + column.getUpdated());
        }
    }

}
