
package com.ebei.canal.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class OracleUtil {
    
    // Oracle url
    private static String URL = ConfigUtils.getConfigValue("application.properties", "oracle.url");
    
    // Oracle 用户名
    private static String USERNAME = ConfigUtils.getConfigValue("application.properties", "oracle.username");
    
    // Oracle 密码
    private static String PASSWORD = ConfigUtils.getConfigValue("application.properties", "oracle.password");
    
    // Oracle 目标写入库
    private static String TODB = ConfigUtils.getConfigValue("application.properties", "oracle.todb");
    
    private static OracleUtil util = null;
    private OracleUtil() {}
    
    public static OracleUtil getInstance() {
        if (util == null) {
            synchronized (OracleUtil.class) {
                if (util == null) {
                    util = new OracleUtil();
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
                // 过滤侨鑫数据，暂时写死
                if ("company_id".equalsIgnoreCase(columnName) && !"222".equals(column.getValue())) {
                    return null;
                }
                shouldSyncColumns.add(column);
            }
        }
        return shouldSyncColumns;
    }
    
    public void oracleInsert(String database, String table, List<CanalEntry.Column> columns) throws SQLException {
        List<CanalEntry.Column> shouldSyncColumns = this.getShouldSync(table, columns);
        if (shouldSyncColumns == null) return;
        StringBuffer sql = new StringBuffer();
        sql.append("insert into \"");
        sql.append(table.toUpperCase());
        sql.append("\"(");
        for (CanalEntry.Column column : shouldSyncColumns) {
            String columnName = column.getName().toUpperCase();
            sql.append("\"" + columnName + "\"");
            if (column==shouldSyncColumns.get(shouldSyncColumns.size()-1)) {
                sql.append(") values(");
            } else {
                sql.append(",");
            }
        }
        for (CanalEntry.Column column : shouldSyncColumns) {
            String columnValue = column.getValue();
            if (column.getIsNull()) {
                sql.append(" NULL ");
            } else {
                if (column.getMysqlType().equalsIgnoreCase("datetime")|| column.getMysqlType().equalsIgnoreCase("date")) {
                    sql.append("to_date('" + columnValue + "', 'YYYY-MM-DD HH24:MI:SS') ");
                } else {
                    sql.append("'" + columnValue + "'");
                }
            }
            if (column==shouldSyncColumns.get(shouldSyncColumns.size()-1)) {
                sql.append(")");
            } else {
                sql.append(",");
            }
        }
        this.exeSql(sql.toString());
    }

    public void oracleUpdate(String database, String table, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> columns) throws SQLException {
        List<CanalEntry.Column> shouldSyncBeforeColumns = this.getShouldSync(table, beforeColumns);
        if (shouldSyncBeforeColumns == null) return;
        List<CanalEntry.Column> shouldSyncColumns = this.getShouldSync(table, columns);
        if (shouldSyncColumns == null) return;
        StringBuffer sql = new StringBuffer();
        sql.append("update \"");
        sql.append(table.toUpperCase());
        sql.append("\" set ");
        for (CanalEntry.Column column : shouldSyncColumns) {
            sql.append("\"" + column.getName().toUpperCase() + "\"");
            sql.append("=");
            if (column.getIsNull()) {
                sql.append(" NULL ");
            } else {
                if (column.getMysqlType().equalsIgnoreCase("datetime") || column.getMysqlType().equalsIgnoreCase("date")) {
                    sql.append("to_date('" + column.getValue() + "', 'YYYY-MM-DD HH24:MI:SS') ");
                } else {
                    sql.append("'" + column.getValue() + "'");
                }
            }
            if (column!=shouldSyncColumns.get(shouldSyncColumns.size()-1)) {
                sql.append(",");
            }
        }
        sql.append(" where ");
        for (CanalEntry.Column column : shouldSyncBeforeColumns) {
            sql.append("\"" + column.getName().toUpperCase() + "\"");
            if (column.getIsNull()) {
                sql.append(" IS NULL ");
            } else {
                sql.append("=");
                if (column.getMysqlType().equalsIgnoreCase("datetime" )|| column.getMysqlType().equalsIgnoreCase("date")) {
                    sql.append("to_date('" + column.getValue() + "', 'YYYY-MM-DD HH24:MI:SS') ");
                } else {
                    sql.append("'" + column.getValue() + "'");
                }
            }
            if (column!=shouldSyncBeforeColumns.get(shouldSyncBeforeColumns.size()-1)) {
                sql.append(" AND ");
            }
        }
        this.exeSql(sql.toString());
    }

    public void oracleDelete(String database, String table, List<CanalEntry.Column> beforeColumns) throws SQLException{
        List<CanalEntry.Column> shouldSyncBeforeColumns = this.getShouldSync(table, beforeColumns);
        if (shouldSyncBeforeColumns == null) return;
        StringBuffer sql = new StringBuffer();
        sql.append("delete from \"");
        sql.append(table.toUpperCase());
        sql.append("\" where ");
        for (CanalEntry.Column column : shouldSyncBeforeColumns) {
            sql.append("\"" + column.getName().toUpperCase() + "\"");
            if (column.getIsNull()) {
                sql.append(" IS NULL ");
            } else {
                sql.append("=");
                if (column.getMysqlType().equalsIgnoreCase("datetime")|| column.getMysqlType().equalsIgnoreCase("date")) {
                    sql.append("to_date('" + column.getValue() + "', 'YYYY-MM-DD HH24:MI:SS') ");
                } else {
                    sql.append("'" + column.getValue() + "'");
                }
            }
            if (column!=shouldSyncBeforeColumns.get(shouldSyncBeforeColumns.size()-1)) {
                sql.append(" AND ");
            }
        }
        this.exeSql(sql.toString());
    }

    public void oracleTruncate(String database, String table) throws SQLException {
        StringBuffer sql = new StringBuffer();
        sql.append("truncate table \"");
        sql.append(table.toUpperCase());
        sql.append("\"");
        this.exeSql(sql.toString());
    }

    private void exeSql(String sql) throws SQLException{
        System.out.println(sql);
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException se) {
            throw new SQLException(se);
        } finally {
            if (conn != null) {
                try {
                    ps.close();
                    conn.close();
                } catch (Exception e) {}
            }
        }
    }
    
}