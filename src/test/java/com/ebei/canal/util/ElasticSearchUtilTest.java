package com.ebei.canal.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

public class ElasticSearchUtilTest {

    public static void main(String[] args) {
        ElasticSearchUtil util = ElasticSearchUtil.getInstance();
        try {
            JSONObject obj = new JSONObject();
//            String id = "c8045261-821b-45c6-b563-37e320cca01c";
//            obj.put("id", id);
//            obj.put("testName","superg");
////            util.addData("canal", "record", obj);
//            System.out.println("1---"+util.getData("canal","record", id));
//            obj.put("testName","super666");
//            obj.put("testName2","superg2");
//            util.updateData("canal","record", obj);
//            System.out.println("2---"+util.getData("canal","record", id));
//            obj = new JSONObject();
//            obj.put("testName2", "superg2");
//            System.out.println("3---"+util.searchData("canal","record", obj));
//            obj.put("testName2", "superg3");
//            System.out.println("4---"+util.searchData("canal","record", obj));
//            System.out.println("5---"+util.searchData("canal","record", null));

            JSONObject times = new JSONObject();
            JSONObject time1 = new JSONObject();
//            time1.put("gte", "2019-12-29 11:45:55");
//            time1.put("lte", "2019-12-31 11:45:55");
//            times.put("create_date", time1);
            JSONObject otherRanges = new JSONObject();
            //otherRanges.put("state", "0-6");
            //otherRanges.put("state2","0-7")
            // String[] keys = {"message_id","user_name"};
            //String[] keywords = {"e"};
            String[] keys = null;
            String[] keywords = null;
            JSONArray id=new JSONArray();
            JSONObject ids=new JSONObject();
            id.add("c");
            id.add("d");
            id.add("f");
            id.add("e");
            ids.put("message_id",id.toArray());
            System.out.println(ids.toJSONString());
            //Object[] ids = {new String[]{"", "", "", ""}};
            JSONObject other = new JSONObject();
            //other.put("message_content", "e");
            //other.put("user_name", "e");
            System.out.println("6---" + util.searchData("blog2", "tb_message", other, ids, keys, keywords, times, otherRanges, 10, 1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
