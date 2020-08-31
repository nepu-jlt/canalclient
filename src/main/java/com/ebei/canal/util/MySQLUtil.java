
package com.ebei.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("Duplicates")
public class MySQLUtil {

    private static Logger logger = LoggerFactory.getLogger(MySQLUtil.class);
    
    // MySQL url
    private static String URL = ConfigUtils.getConfigValue("application.properties", "mysql.url");
    
    // MySQL 用户名
    private static String USERNAME = ConfigUtils.getConfigValue("application.properties", "mysql.username");
    
    // MySQL 密码
    private static String PASSWORD = ConfigUtils.getConfigValue("application.properties", "mysql.password");
    
    // MySQL 目标写入库
    private static String TODB = ConfigUtils.getConfigValue("application.properties", "mysql.todb");

    // MySQL 新增方式(insert or replace)
    private static String MYSQL_INSERT = ConfigUtils.getConfigValue("application.properties", "mysql.insert");
    
    private static MySQLUtil util = null;

    private MySQLUtil() {}
    
    public static MySQLUtil getInstance() {
        if (util == null) {
            synchronized (MySQLUtil.class) {
                if (util == null) {
                    util = new MySQLUtil();
                }
            }
        }
        return util;
    }

    private List<CanalEntry.Column> getShouldSync(String table, List<CanalEntry.Column> columns) {
        // 查找该表的同步模式配置
        String tableMode = ConfigUtils.getConfigValue("application.properties", "table." + table + ".mode");
        if (StringUtils.isEmpty((tableMode))) {
            tableMode = "black";
        }
        // 查找该表的忽略/同步列名配置
        String tableColumns = ConfigUtils.getConfigValue("application.properties", "table." + table + ".columns");

        // 过滤需要同步的列
        List<CanalEntry.Column> shouldSyncColumns = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            String columnName = column.getName().toUpperCase();
            boolean shouldSync = true;
            if ("black".equals(tableMode)) {
                if (StringUtils.isNotEmpty(tableColumns)) {
                    for (String tableIgnoreColumn : tableColumns.split(",")) {
                        if (tableIgnoreColumn.trim().equalsIgnoreCase(columnName)) {
                            shouldSync = false;
                            break;
                        }
                    }
                }
            }
            if ("white".equals(tableMode)) {
                shouldSync = false;
                if (StringUtils.isNotEmpty(tableColumns)) {
                    for (String tableShouldColumn : tableColumns.split(",")) {
                        if (tableShouldColumn.trim().equalsIgnoreCase(columnName)) {
                            shouldSync = true;
                            break;
                        }
                    }
                }
            }
            if (shouldSync) {
                shouldSyncColumns.add(column);
            }
        }
        return shouldSyncColumns;
    }
    
    public void mySQLInsert(String database, String table, List<CanalEntry.Column> columns) throws SQLException {
        List<CanalEntry.Column> shouldSyncColumns = this.getShouldSync(table, columns);
        if (shouldSyncColumns == null) return;
        StringBuffer sql = new StringBuffer();
        // 判断是覆盖式同步还是非覆盖式
        if ("replace".equals(MYSQL_INSERT)) {
            sql.append("replace into ");
        } else {
            sql.append("insert into ");
        }
        sql.append(table);
        sql.append("(");
        for (CanalEntry.Column column : shouldSyncColumns) {
            String columnName = column.getName();
            sql.append("`" + columnName + "`");
            sql.append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values(");
        for (CanalEntry.Column column : shouldSyncColumns) {
            String columnValue = column.getValue();
            if (column.getIsNull()) {
                sql.append(" NULL ");
            } else {
                // 如果是数字int bigint bit则不需要引号
                if (column.getMysqlType().contains("bit")
                        || column.getMysqlType().contains("int")) {
                    sql.append(columnValue);
                } else {
                    sql.append("'" + columnValue + "'");
                }
            }
            sql.append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        logger.info("insert sql:" + sql.toString());
        this.exeSql(sql.toString());
    }
    
    public void mySQLUpdate(String database, String table, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> columns) throws SQLException {
        List<CanalEntry.Column> shouldSyncBeforeColumns = this.getShouldSync(table, beforeColumns);
        if (shouldSyncBeforeColumns == null) return;
        List<CanalEntry.Column> shouldSyncColumns = this.getShouldSync(table, columns);
        if (shouldSyncColumns == null) return;
        StringBuffer sql = new StringBuffer();
        sql.append("update ");
        sql.append(table);
        sql.append(" set ");
        boolean columns4Update = false;
        for (int i = 0; i< shouldSyncColumns.size(); i++) {
            // 比对前后列的值，只有列值改变时才同步
            CanalEntry.Column column = shouldSyncColumns.get(i);
            CanalEntry.Column beforeColumn = shouldSyncBeforeColumns.get(i);
            if (column.getName().equals(beforeColumn.getName())) {
                if (column.getIsNull() && beforeColumn.getIsNull()) {
                    continue;
                } else if ((column.getIsNull() && !beforeColumn.getIsNull())
                        || (!column.getIsNull() && beforeColumn.getIsNull()) || !column.getValue().equals(beforeColumn.getValue())) {
                    sql.append("`" + column.getName() + "`");
                    sql.append("=");
                    if (column.getIsNull()) {
                        sql.append(" NULL ");
                    } else {
                        // 如果是数字int bigint bit则不需要引号
                        if (column.getMysqlType().contains("bit")
                                || column.getMysqlType().contains("int")) {
                            sql.append(column.getValue());
                        } else {
                            sql.append("'" + column.getValue() + "'");
                        }
                    }
                    sql.append(",");
                    columns4Update = true;
                }
            } else {
                logger.error("column names don't match! before:" + beforeColumn.getName() + ", after:" + column.getName());
            }
        }
        if (!columns4Update) {
            return;
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" where 1=1 ");
        StringBuffer sqlWhere = new StringBuffer();
        boolean hasKey = false;
        for (CanalEntry.Column column : shouldSyncBeforeColumns) {
            // 有key的情况下仅匹配key；无key全字段匹配
            if (column.getIsKey()) {
                if (!hasKey) {
                    hasKey = true;
                    sqlWhere = new StringBuffer();
                }
                sqlWhere.append(" AND ");
                sqlWhere.append("`" + column.getName() + "`");
                if (column.getIsNull()) {
                    sqlWhere.append(" IS NULL ");
                } else {
                    sqlWhere.append("=");
                    sqlWhere.append("'" + column.getValue() + "'");
                }
            } else {
                if (!hasKey) {
                    sqlWhere.append(" AND ");
                    sqlWhere.append("`" + column.getName() + "`");
                    if (column.getIsNull()) {
                        sqlWhere.append(" IS NULL ");
                    } else {
                        sqlWhere.append("=");
                        sqlWhere.append("'" + column.getValue() + "'");
                    }
                }
            }
        }
        sql.append(sqlWhere.toString());
        logger.info("update sql:" + sql.toString());
        this.exeSql(sql.toString());
    }
    
    public void mySQLDelete(String database, String table, List<CanalEntry.Column> beforeColumns) throws SQLException{
        List<CanalEntry.Column> shouldSyncBeforeColumns = this.getShouldSync(table, beforeColumns);
        if (shouldSyncBeforeColumns == null) return;
        StringBuffer sql = new StringBuffer();
        sql.append("delete from ");
        sql.append(table);
        sql.append(" where 1=1 ");
        StringBuffer sqlWhere = new StringBuffer();
        boolean hasKey = false;
        for (CanalEntry.Column column : shouldSyncBeforeColumns) {
            // 有key的情况下仅匹配key；无key全字段匹配
            if (column.getIsKey()) {
                if (!hasKey) {
                    hasKey = true;
                    sqlWhere = new StringBuffer();
                }
                sqlWhere.append(" AND ");
                sqlWhere.append("`" + column.getName() + "`");
                if (column.getIsNull()) {
                    sqlWhere.append(" IS NULL ");
                } else {
                    sqlWhere.append("=");
                    sqlWhere.append("'" + column.getValue() + "'");
                }
            } else {
                if (!hasKey) {
                    sqlWhere.append(" AND ");
                    sqlWhere.append("`" + column.getName() + "`");
                    if (column.getIsNull()) {
                        sqlWhere.append(" IS NULL ");
                    } else {
                        sqlWhere.append("=");
                        sqlWhere.append("'" + column.getValue() + "'");
                    }
                }
            }
        }

        sql.append(sqlWhere.toString());
        logger.info("delete sql:" + sql.toString());
        this.exeSql(sql.toString());
    }
    
    public void mySQLTruncate(String database, String table) throws SQLException {
        StringBuffer sql = new StringBuffer();
        sql.append("truncate table ");
        sql.append(table);
        this.exeSql(sql.toString());
    }

    private void exeSql(String sql) throws SQLException{
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL + TODB, USERNAME, PASSWORD);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException se) {
            throw new SQLException(se);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {}
            }
        }
    }
    
}