
package com.ebei.canal.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.ebei.canal.vo.EsParams;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ElasticSearchUtil {

    private final Logger log = LoggerFactory.getLogger(ElasticSearchUtil.class);

    private final static String ES_URL = ConfigUtils.getConfigValue("application.properties", "es.host");

    private final static Integer ES_PORT = Integer.parseInt(ConfigUtils.getConfigValue("application.properties", "es.port"));

    private final static String ES_PROTOCOL = ConfigUtils.getConfigValue("application.properties", "es.protocol");

    private final static String ES_USERNAME = "";

    private final static String ES_PASSWORD = "";

    private final static String MYSQL_URL = ConfigUtils.getConfigValue("application.properties", "mysql.url");

    private final static String MYSQL_USERNAME = ConfigUtils.getConfigValue("application.properties", "mysql.username");

    private final static String MYSQL_PASSWORD = ConfigUtils.getConfigValue("application.properties", "mysql.password");

    private static ElasticSearchUtil util = null;

    private ElasticSearchUtil() {
    }

    public static ElasticSearchUtil getInstance() {
        if (util == null) {
            synchronized (ElasticSearchUtil.class) {
                if (util == null) {
                    util = new ElasticSearchUtil();
                    util.buildEsClient();
                }
            }
        }
        return util;
    }

    private RestClient client = null;

    public RestClient buildEsClient() {
        try {

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(ES_USERNAME, ES_PASSWORD));
            client = RestClient.builder(new HttpHost(ES_URL, ES_PORT, ES_PROTOCOL))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    }).build();
            log.info("连接ES");
            return client;
        } catch (Exception e) {
            log.info("连接ES失败！" + e.getMessage());
        }

        return null;
    }

    /**
     * 创建索引
     *
     * @throws IOException
     */
    public void createIndex(String index) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        Response response = client.performRequest("PUT", "/" + index, params);
        StatusLine statusLine = response.getStatusLine();
        log.info("PUT:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    public void deleteIndex(String index) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        Response response = client.performRequest("DELETE", "/" + index, params);
        StatusLine statusLine = response.getStatusLine();
        log.info("DELETE:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
    }

    /**
     * 添加数据
     *
     * @throws IOException
     */
    public void addData(String index, String type, JSONObject jsonObj) throws IOException {
        if (StringUtils.isEmpty(type) || jsonObj == null) {
            return;
        }
        Map<String, String> params = Collections.emptyMap();
        String id = null;
        // 查找该表的同步模式配置
        String tableMode = ConfigUtils.getConfigValue("application.properties", "table." + type + ".mode");
        if (StringUtils.isEmpty((tableMode))) {
            tableMode = "black";
        }
        // 查找该表的主键列名配置
        String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + type + ".key");
        if (StringUtils.isEmpty(tableIdColumn)) {
            tableIdColumn = "id";
        }
        if (jsonObj.containsKey(tableIdColumn)) {
            id = jsonObj.getString(tableIdColumn);
        }
        // 查找该表的忽略/同步列名配置
        String tableColumns = ConfigUtils.getConfigValue("application.properties", "table." + type + ".columns");
        if ("black".equals(tableMode)) {
            if (StringUtils.isNotEmpty(tableColumns)) {
                for (String tableIgnoreColumn : tableColumns.split(","))
                    jsonObj.remove(tableIgnoreColumn);
            }
        }
        if ("white".equals(tableMode)) {
            JSONObject jsonObjWhite = new JSONObject();
            if (StringUtils.isNotEmpty(tableColumns)) {
                for (String tableShouldColumn : tableColumns.split(",")) {
                    if (jsonObj.containsKey(tableShouldColumn))
                    jsonObjWhite.put(tableShouldColumn, jsonObj.get(tableShouldColumn));
                }
            }
            jsonObj = jsonObjWhite;
        }
        HttpEntity entity = new StringEntity(jsonObj.toString(), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", "/" + index + "/" + type + "/" + (id == null ? "" : id), params, entity);
        StatusLine statusLine = response.getStatusLine();
        log.info("POST:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    public void deleteData(String index, String type, String id) throws IOException {
        if (StringUtils.isEmpty(type) || StringUtils.isEmpty(id)) {
            return;
        }
        Map<String, String> params = Collections.emptyMap();
        Response response = client.performRequest("DELETE", "/" + index + "/" + type + "/" + id, params);
        StatusLine statusLine = response.getStatusLine();
        log.info("DELETE:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
    }

    /**
     * 读取数据
     *
     * @throws IOException
     */
    public String getData(String index, String type, String id) throws IOException {
        if (StringUtils.isEmpty(type) || StringUtils.isEmpty(id)) {
            return null;
        }
        Map<String, String> params = Collections.emptyMap();
        Response response = client.performRequest("GET", "/" + index + "/" + type + "/" + id, params);
        if (response != null) {
            StatusLine statusLine = response.getStatusLine();
            log.info("GET:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
            String result = EntityUtils.toString(response.getEntity());
            if (StringUtils.isNotEmpty(result)) {
                return result;
            }
        }
        return null;
    }

    /**
     * 更新数据
     *
     * @throws IOException
     */
    public void updateData(String index, String type, JSONObject jsonObj) throws IOException {
        if (StringUtils.isEmpty(type) || jsonObj == null) {
            return;
        }
        Map<String, String> params = Collections.emptyMap();
        String id = null;


        // 查找该表的同步模式配置
        String tableMode = ConfigUtils.getConfigValue("application.properties", "table." + type + ".mode");
        if (StringUtils.isEmpty((tableMode))) {
            tableMode = "black";
        }
        // 查找该表的主键列名配置
        String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + type + ".key");
        if (StringUtils.isEmpty(tableIdColumn)) {
            tableIdColumn = "id";
        }
        if (jsonObj.containsKey(tableIdColumn)) {
            id = jsonObj.getString(tableIdColumn);
        }
        if (id == null) {
            return;
        }
        // 查找该表的忽略/同步列名配置
        String tableColumns = ConfigUtils.getConfigValue("application.properties", "table." + type + ".columns");
        if ("black".equals(tableMode)) {
            if (StringUtils.isNotEmpty(tableColumns)) {
                for (String tableIgnoreColumn : tableColumns.split(","))
                    jsonObj.remove(tableIgnoreColumn);
            }
        }
        if ("white".equals(tableMode)) {
            JSONObject jsonObjWhite = new JSONObject();
            if (StringUtils.isNotEmpty(tableColumns)) {
                for (String tableShouldColumn : tableColumns.split(",")) {
                    if (jsonObj.containsKey(tableShouldColumn))
                        jsonObjWhite.put(tableShouldColumn, jsonObj.get(tableShouldColumn));
                }
            }
            jsonObj = jsonObjWhite;
        }


        HttpEntity entity = new StringEntity(jsonObj.toString(), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", "/" + index + "/" + type + "/" + id, params, entity);
        StatusLine statusLine = response.getStatusLine();
        log.info("POST:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
    }

//    /**
//     * 搜索数据
//     * @throws IOException
//     */
//    public String searchData(String index, String type, JSONObject jsonObj, Integer pageSize, Integer pageIndex) throws IOException {
//        if (StringUtils.isEmpty(type)) {
//            return null;
//        }
//        Map<String, String> params = Collections.emptyMap();
//        JSONObject queryJson = new JSONObject();
//        if (jsonObj != null) {
//            JSONObject matchJson = new JSONObject();
//            matchJson.put("match", jsonObj);
//            queryJson.put("query", matchJson);
//        }
//        HttpEntity entity = new StringEntity(queryJson.toString(), ContentType.APPLICATION_JSON);
//        if (pageSize == null) {
//            pageSize = 100;
//        }
//        if (pageIndex == null) {
//            pageIndex = 1;
//        }
//        int from=(pageIndex-1)*pageSize;
//        Response response = client.performRequest("POST", "/"+index + "/" + type + "/_search?size=" + pageSize + "&from="+from, params, entity);
//        if (response != null) {
//            StatusLine statusLine = response.getStatusLine();
//            log.info("POST:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
//            String result = EntityUtils.toString(response.getEntity());
//            if (StringUtils.isNotEmpty(result)) {
//                return result;
//            }
//        }
//        return null;
//    }

    /**
     * 搜索数据
     *
     * @throws IOException
     */
    public String searchData(String index, String type, JSONObject jsonObj, JSONObject ids, Object[] keys, Object[] keywords, JSONObject times, JSONObject otherRanges, Integer pageSize, Integer pageIndex) throws IOException {
        if (StringUtils.isEmpty(type)) {
            return null;
        }
        Map<String, String> params = Collections.emptyMap();
        JSONObject paramObject = new JSONObject();
        JSONArray fields = new JSONArray();

        JSONObject queryJson = new JSONObject();
        JSONArray mustA = new JSONArray();
        JSONObject boolF = new JSONObject();
        if (keys != null && keywords != null) {
            for (Object key : keys) {
                fields.add(key);
                JSONObject nof = new JSONObject();
                nof.put("number_of_fragments",0);
                boolF.put(key.toString(), nof);
            }
            paramObject.put("fields", fields);
            paramObject.put("type", "cross_fields");
            String minimumShouldMatch = ConfigUtils.getConfigValue("application.properties", "query.minimum_should_match");
            if (StringUtils.isEmpty(minimumShouldMatch)) {
                minimumShouldMatch = "100%";
            }
            paramObject.put("minimum_should_match", minimumShouldMatch);
            paramObject.put("query", StringUtils.join(keywords, " "));
            JSONObject multiJ = new JSONObject();
            multiJ.put("multi_match", paramObject);
            mustA.add(multiJ);
        }


        //in 查询，当集合中字段有"-","_"等特殊字符时，key名需加keyword
        JSONArray filterB = new JSONArray();
//        in 查询eg:id in ("dsf","123")
        if (ids != null) {
            for (String idArray : ids.keySet()) {
                JSONObject idFilter = new JSONObject();
                JSONObject id = new JSONObject();
                id.put(idArray, ids.get(idArray));
                idFilter.put("terms", id);
                filterB.add(idFilter);
            }
        }
        //时间范围处理
        //filter
        if (times != null && !"".equals(times.toString())) {
            //时间范围
            //遍历timeJson取出其中的键跟值，放入range中，键是查询的时间名称，值两个时间，表示一个范围

            for (String time1 : times.keySet()) {
                //具体时间，time的键为要比较的字段，值为两个范围
                JSONObject range = new JSONObject();
                JSONObject filterA = new JSONObject();
                JSONObject time = times.getJSONObject(time1);
                //为空值删除条件
                if (time.get("gte") == null || "".equals(time.get("gte"))) {
                    time.remove("gte");
                }
                if (time.get("lte") == null || "".equals(time.get("lte"))) {
                    time.remove("lte");
                }
//                if (time != null && !"".equals(time.toString())) {
//                    time.put("format", "yyyy-MM-dd HH:mm:ss");
//                    //time.put("time_zone", "+08:00");
//                }
                //封装时间为 :"字段" : {"gte": "01/01/2012","lte": "2013","format": "yyyy-MM-dd hh:mm:ss"}
                //加.keyword是因为数据库里从的string，不加范围有问题。
                range.put(time1 + ".keyword", time);
                //时间放入filter中
                filterA.put("range", range);
                log.info(filterA.toString());
                filterB.add(filterA);
            }
        }

        //其他范围处理 类似1-7这种参数值
        if (otherRanges != null && !"".equals(otherRanges.toString())) {
            for (String rangeName : otherRanges.keySet()) {
                JSONObject rangeA = new JSONObject();
                JSONObject rangeJson = new JSONObject();
                JSONObject filterC = new JSONObject();
                try {
                    String gte = otherRanges.get(rangeName).toString().split("-")[0];
                    String lte = otherRanges.get(rangeName).toString().split("-")[1];
                    rangeJson.put("gte", gte);
                    rangeJson.put("lte", lte);
                    rangeA.put(rangeName, rangeJson);
                    filterC.put("range", rangeA);
                    filterB.add(filterC);
                } catch (Exception e) {
                    log.info("参数错误");
                    return "参数错误";
                }

            }
        }

        if (jsonObj != null) {
            for (String key : jsonObj.keySet()) {
                JSONObject keyJson = new JSONObject();
                keyJson.put(key, jsonObj.get(key).toString().toLowerCase());
//                JSONObject matchJson = new JSONObject();
//                matchJson.put("match", keyJson);
//                mustA.add(matchJson);

                JSONObject filterC = new JSONObject();
                filterC.put("term", keyJson);
                filterB.add(filterC);
            }
        }
        JSONObject mustJ = new JSONObject();
        mustJ.put("must", mustA);
        //添加根据时间筛选、根据类似1-7这种范围筛选
        mustJ.put("filter", filterB);
        JSONObject boolJ = new JSONObject();
        boolJ.put("bool", mustJ);
        queryJson.put("query", boolJ);
        // 高亮关键字
        JSONObject boolH = new JSONObject();
        boolH.put("fields", boolF);
        queryJson.put("highlight", boolH);
        log.info(queryJson.toString());
        if (pageSize == null) {
            pageSize = 100;
        }
        if (pageIndex == null) {
            pageIndex = 1;
        }
        int from = (pageIndex - 1) * pageSize;
        HttpEntity entity = new StringEntity(queryJson.toString(), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", "/" + index + "/" + type + "/_search?size=" + pageSize + "&from=" + from, params, entity);
        if (response != null) {
            StatusLine statusLine = response.getStatusLine();
            log.info("searchData:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
            String result = EntityUtils.toString(response.getEntity());
            if (StringUtils.isNotEmpty(result)) {
                return result;
            }
        }
        return null;
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    public void deleteByQuery(String index, String type, JSONObject jsonObj) throws IOException {
        if (StringUtils.isEmpty(type)) {
            return;
        }
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new StringEntity(jsonObj.toString(), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", "/" + index + "/" + type + "/_delete_by_query?conflicts=proceed", params, entity);
        if (response != null) {
            StatusLine statusLine = response.getStatusLine();
            log.info("deleteByQuery:[" + statusLine.getStatusCode() + "]" + statusLine.toString());
        }
    }

    public void esDelete(String database, String table, List<CanalEntry.Column> columns) {
        if (columns == null) {
            return;
        }
        // 查找该表的主键列名配置
        String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + table + ".key");
        if (StringUtils.isEmpty(tableIdColumn)) {
            tableIdColumn = "id";
        }
        try {
            for (CanalEntry.Column column : columns) {
                if (tableIdColumn.equalsIgnoreCase(column.getName())) {
                    this.deleteData(database, table, column.getValue());
                    return;
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void esInsert(String database, String table, List<CanalEntry.Column> columns) {
        if (columns == null) {
            return;
        }
        JSONObject json = new JSONObject();
        try {
            for (CanalEntry.Column column : columns) {
                // 判断数据类型
                if (StringUtils.isEmpty(column.getValue())) {
                    json.put(column.getName(), "");
                } else if (column.getMysqlType().contains("int") || column.getMysqlType().contains("INT")) {
                    try {
                        json.put(column.getName(), Integer.parseInt(column.getValue()));
                    } catch (Exception e) {
                        json.put(column.getName(), Long.parseLong(column.getValue()));
                    }
                } else if (column.getMysqlType().contains("float") || column.getMysqlType().contains("FLOAT")) {
                    json.put(column.getName(), Float.parseFloat(column.getValue()));
                } else if (column.getMysqlType().contains("decimal") || column.getMysqlType().contains("DECIMAL")
                        || column.getMysqlType().contains("double") || column.getMysqlType().contains("DOUBLE")) {
                    json.put(column.getName(), Double.parseDouble(column.getValue()));
                } else {
                    json.put(column.getName(), column.getValue());
                }
            }
            this.addData(database, table, json);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void esUpdate(String database, String table, List<CanalEntry.Column> columns) {
        if (columns == null) {
            return;
        }
        JSONObject json = new JSONObject();
        try {
            for (CanalEntry.Column column : columns) {
                json.put(column.getName(), column.getValue());
            }
            this.updateData(database, table, json);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void esTruncate(String database, String table) {
        JSONObject json = new JSONObject();
        try {
            String matchAllStr = "{\"query\": {\"match_all\": {}}}";
            this.deleteByQuery(database, table, JSONObject.parseObject(matchAllStr));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String read(EsParams params) throws IOException {
        //除params的其他参数
        JSONObject resultJ = null;
        if (params.getQueryType() == null || params.getQueryType() == 1) {
            // 查找该表的主键列名配置
            String tableIdColumn = ConfigUtils.getConfigValue("application.properties", "table." + params.getTable() + ".key");
            if (StringUtils.isEmpty(tableIdColumn)) {
                tableIdColumn = "id";
            }
            resultJ = JSONObject.parseObject(this.getData(params.getDatabase(), params.getTable(), params.getParams().getString(tableIdColumn)));
            if (resultJ != null) {
                JSONArray convertedA = new JSONArray();
                convertedA.add(resultJ.getJSONObject("_source"));
                // 分页数据组装
                JSONObject result = new JSONObject();
                result.put("total", 1);
                result.put("rows", convertedA);
                return result.toString();
            }
        }

        //参数处理keys，keywords
        JSONObject paramsObj = params.getParams();
        JSONObject musts = new JSONObject();
        if (params.getQueryType() == 2) {
            JSONArray keys = null;
            if (paramsObj.containsKey("keys")) {
                keys = paramsObj.getJSONArray("keys");
                paramsObj.remove("keys");
            }
            JSONArray keywords = null;
            if (paramsObj.containsKey("keywords")) {
                keywords = paramsObj.getJSONArray("keywords");
                paramsObj.remove("keywords");
            }

            //参数处理，值中带type的所有参数（key，value形式）
            JSONObject times  = new JSONObject();
            JSONObject otherRanges = new JSONObject();
            JSONObject ids = new JSONObject();
            for (String key : paramsObj.keySet()) {
                try {
                    //获取没有type的key，value值放入musts中，没有type会报错。报错是直接放入musts
                    String isHaveType = ((JSONObject) JSON.toJSON(paramsObj.get(key))).get("type").toString();
                } catch (Exception e) {
                    musts.put(key, paramsObj.get(key));
                    continue;
                }

                if ("timeRange".equals(((JSONObject) JSON.toJSON(paramsObj.get(key))).get("type").toString())) {
                    JSONObject time = new JSONObject();
                    time.put("gte", ((JSONObject) JSON.toJSON(paramsObj.get(key))).get("start").toString());
                    time.put("lte", ((JSONObject) JSON.toJSON(paramsObj.get(key))).get("end").toString());
                    times.put(key, time);
                    //paramsObj.remove(key);
                }
//                数字范围，暂不需要，有点问题。  问题：取出来num1为null，num2为第一个值，   用get（2）有报越界。
                else if ("countRange".equals(((JSONObject) JSON.toJSON(paramsObj.get(key))).get("type").toString())) {

                    Object num1 = ((JSONObject) JSON.toJSON(paramsObj.get(key))).get("start");
                    Object num2 = ((JSONObject) JSON.toJSON(paramsObj.get(key))).get("end");
                    if (num1 != null && num2 != null) {
                        otherRanges.put(key, num1.toString() + "-" + num2.toString());
                    }
                } else if ("findIn".equals(((JSONObject) JSON.toJSON(paramsObj.get(key))).get("type").toString())) {
                    ids.put(key, ((JSONObject) JSON.toJSON(paramsObj.get(key))).getJSONArray("value"));
                }
            }
            // 排序
            resultJ = JSONObject.parseObject(this.searchData(params.getDatabase(), params.getTable(), musts, ids, keys == null ? null : keys.toArray(), keywords == null ? null : keywords.toArray(), times, otherRanges, params.getPageSize(), params.getPageIndex()));
            if (resultJ != null && resultJ.containsKey("hits") && resultJ.getJSONObject("hits").containsKey("hits")) {
                JSONArray resultA = resultJ.getJSONObject("hits").getJSONArray("hits");
                JSONArray convertedA = new JSONArray();
                for (int i = 0; i < resultA.size(); i++) {
                    JSONObject _source = resultA.getJSONObject(i).getJSONObject("_source");
                    JSONObject _highlight = resultA.getJSONObject(i).getJSONObject("highlight");
                    JSONObject highlight = new JSONObject();
                    if (_highlight != null)
                        for (String key : _highlight.keySet()) {
                            if (_highlight.getJSONArray(key).size() > 0) {
                                highlight.put(key, _highlight.getJSONArray(key).get(0));
                            }
                        }
                    _source.put("highlight", highlight);
                    convertedA.add(_source);
                }
                // 分页数据组装
                JSONObject result = new JSONObject();
                result.put("total", resultJ.getJSONObject("hits").getString("total"));
                result.put("rows", convertedA);
                return result.toString();
            }
        }
        return null;
    }

    public Integer sync(EsParams params) throws IOException {
        String databaseName = params.getDatabase();
        if (StringUtils.isEmpty(databaseName)) {
            return null;
        }
        String tableName = params.getTable();
        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        String columnName = params.getParams().getString("column");
        if (StringUtils.isEmpty(columnName)) {
            return null;
        }
        String startTime = params.getParams().getString("start");
        String endTime = params.getParams().getString("end");
        try {
            Connection conn = DriverManager.getConnection(MYSQL_URL + databaseName, MYSQL_USERNAME, MYSQL_PASSWORD);
            StringBuffer sqlStr = new StringBuffer();
            sqlStr.append("SELECT * FROM " + tableName + " WHERE 1=1 ");
            if (StringUtils.isNotEmpty(startTime)) {
                sqlStr.append(" AND " + columnName + " >= '" + startTime + "'");

            }
            if (StringUtils.isNotEmpty(endTime)) {
                sqlStr.append(" AND " + columnName + " <= '" + endTime + "'");
            }
            if ("tbb_building_ques".equals(tableName)) {
                // 工单任务仅同步3,5,6,9的几个类型
                sqlStr.append(" AND bi_problem_category in (3,5,6,9) ");
            }
            log.info("sync sql:" + sqlStr.toString());
            PreparedStatement ps = conn.prepareStatement(sqlStr.toString());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                JSONObject json = new JSONObject();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    // 判断数据类型
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colType = rsmd.getColumnType(i);
                    String colName = rsmd.getColumnName(i);
                    String colValue = rs.getString(i);
                    if (StringUtils.isEmpty(colValue)) {
                        json.put(colName, "");
                    } else if (colType == Types.BIGINT || colType == Types.INTEGER
                            || colType == Types.SMALLINT || colType == Types.TINYINT) {
                        try {
                            json.put(colName, Integer.parseInt(colValue));
                        } catch (Exception e) {
                            json.put(colName, Long.parseLong(colValue));
                        }
                    } else if (colType == Types.FLOAT) {
                        json.put(colName, Float.parseFloat(colValue));
                    } else if (colType == Types.DOUBLE || colType == Types.DECIMAL) {
                        json.put(colName, Double.parseDouble(colValue));
                    } else {
                        json.put(colName, colValue);
                    }
                }
                this.addData(databaseName, tableName, json);
            }
            return 1;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    public List<String> getExtWords(String dbName) throws IOException {
        if (StringUtils.isEmpty(dbName)) {
            return null;
        }
        List<String> extWords = new ArrayList<>();
        String tableName = "ext_word";
        try {
            Connection conn = DriverManager.getConnection(MYSQL_URL + dbName, MYSQL_USERNAME, MYSQL_PASSWORD);
            StringBuffer sqlStr = new StringBuffer();
            sqlStr.append("SELECT * FROM " + tableName + " WHERE 1=1 ");
            log.info("sync sql:" + sqlStr.toString());
            PreparedStatement ps = conn.prepareStatement(sqlStr.toString());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                extWords.add(rs.getString("word_text"));
            }
            return extWords;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

}