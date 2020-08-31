package com.ebei.canal.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ebei.canal.common.StatusCode;
import com.ebei.canal.common.StatusCodeDesc;
import com.ebei.canal.util.ElasticSearchUtil;
import com.ebei.canal.vo.EsParams;
import com.ebei.canal.vo.ResultVO;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Es Controller
 *
 * @author Victor.You@ebeitech
 * @version 1.0
 * @created 2018/6/1
 */

@RestController
@RequestMapping("/es")
public class ESController {

    private static final Logger logger = LoggerFactory.getLogger(ESController.class);

    private ElasticSearchUtil esUtil;

    @PostMapping("/read")
    public ResultVO<JSONObject> read(@RequestBody EsParams esParams) throws IOException {
        esUtil = ElasticSearchUtil.getInstance();
        Object value = esUtil.read(esParams);
        if (value != null) {
            JSONObject jsonResult = JSONObject.parseObject(value.toString());
            return new ResultVO<>(StatusCode.SUCCESS, StatusCodeDesc.SUCCESS_DESC, jsonResult);
        }
        return new ResultVO<>(StatusCode.NO_DATA, StatusCodeDesc.SUCCESS_DESC);
    }

    /**
     * 同步数据库最新表数据
     *
     * @param esParams
     * @return
     * @throws IOException
     */
    @PostMapping("/sync")
    public ResultVO<Integer> sync(@RequestBody EsParams esParams) throws IOException {
        esUtil = ElasticSearchUtil.getInstance();
        Integer result = esUtil.sync(esParams);
        if (result != null) {
            return new ResultVO<>(StatusCode.SUCCESS, StatusCodeDesc.SUCCESS_DESC, result);
        }
        return new ResultVO<>(StatusCode.ERROR, StatusCodeDesc.ERROR_DESC);
    }

    /**
     * es热更新扩展词（自定义词）
     *
     * @param resp
     * @return
     * @throws IOException
     */
    @GetMapping("/ext.dict/{dbName}")
    public void extDict(HttpServletResponse resp, @PathVariable(name = "dbName") String dbName) {
        esUtil = ElasticSearchUtil.getInstance();
        resp.setContentType("text/html;charset=utf-8");
        PrintWriter writer = null;
        try {
            resp.addDateHeader("Last-Modified", System.currentTimeMillis());
            List<String> extWords = esUtil.getExtWords(dbName);
            writer = resp.getWriter();
            for (String extWord : extWords) {
                writer.println(extWord);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Get ext dict error.");
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    //areaId:公司名称，taskSource：请求来源，builderId：维修单位，typeReport：任务类型  exception：异常审批，oriUserId：受理人，callBackState：回访状态
    // autoFlag:是否激活  quesTaskState: 任务状态（已完成，待派单。。。）startIndex：页码，pageSize：每页数量  serverId：问题分类，checkTaskId：交付批次，projectId，quesRangeId，buildingId，infoId
    //contactPhoneORcontaceName:联系人电话或姓名，quesDesc：任务内容/补充说明/单号
    //closeBeginDate:任务关闭开始时间，closeEndDate: 任务关关闭结束时间，submitBeginDate：受理开始时间，submitEndDate：受理结束时间，finishBeginDate：任务完成开始时间，finishEndDate：任务完成结束时间，
    //answerDayCount:答复超时时间，overDayCount：处理超时时间

    @PostMapping("/find")
    public ResultVO<JSONObject> find(
            String areaId, String projectId, String quesRangeId, String buildingId,
            String infoId, String contactPhoneORcontactName, String closeBeginDate, String closeEndDate,
            String oriUserId, String followUpProcessorId, String server, String typeReport,
            String quesDesc, String quesTaskCode, String taskSource, String builderId,
            String builderOvertime, String exception, String answerDayCount, String overDayCount,
            String callBackState, String autoFlag, String exportFlag, String userId,
            String quesTaskState,
            String isGiveOut, String state, String finishBeginDate, String checkTaskId,
            String submitBeginDate, String submitEndDate, String finishEndDate, Integer startIndex,
            Integer pageSize) throws IOException {

        EsParams esParams = new EsParams();
        JSONObject param = new JSONObject();
        if (!StringUtil.isNullOrEmpty(areaId)) {
            param.put("area_id", areaId);
        }
        if (!StringUtil.isNullOrEmpty(projectId)) {
            param.put("project_id", projectId);
        }
        if (!StringUtil.isNullOrEmpty(quesRangeId)) {
            param.put("ques_range_id", quesRangeId);
        }
        if (!StringUtil.isNullOrEmpty(buildingId)) {
            param.put("building_id", buildingId);
        }
        if (!StringUtil.isNullOrEmpty(infoId)) {
            param.put("info_id", infoId);
        }
        if (!StringUtil.isNullOrEmpty(oriUserId)) {
            param.put("ori_userid", oriUserId);
        }
        if (!StringUtil.isNullOrEmpty(followUpProcessorId)) {
            param.put("followup_processor", followUpProcessorId);
        }
        if (!StringUtil.isNullOrEmpty(typeReport)) {
            param.put("task_type", typeReport);
        }
        if (!StringUtil.isNullOrEmpty(quesTaskCode)) {
            param.put("quesTask_code", quesTaskCode);
        }
        if (!StringUtil.isNullOrEmpty(taskSource)) {
            param.put("task_source", taskSource);
        }
        if (!StringUtil.isNullOrEmpty(builderId)) {
            param.put("builder_id", builderId);
        }
//        if (!StringUtil.isNullOrEmpty(areaId)) {
//            param.put("exception", exception);
//        }
        if (!StringUtil.isNullOrEmpty(callBackState)) {
            param.put("call_back_state", callBackState);
        }
        if (!StringUtil.isNullOrEmpty(autoFlag)) {
            param.put("auto_flag", autoFlag);
        }
//        if (!StringUtil.isNullOrEmpty(areaId)) {
//            param.put("export_flag", exportFlag);
//        }
        if (!StringUtil.isNullOrEmpty(quesTaskState)) {
            param.put("quseTask_state", quesTaskState);
        }
        if (!StringUtil.isNullOrEmpty(isGiveOut)) {
            param.put("is_give_out", isGiveOut);
        }
        if (!StringUtil.isNullOrEmpty(state)) {
            param.put("state", state);
        }
        if (!StringUtil.isNullOrEmpty(checkTaskId)) {
            param.put("check_task_id", checkTaskId);
        }

        if (!StringUtil.isNullOrEmpty(startIndex.toString())) {
            esParams.setPageIndex(startIndex);
        } else {
            esParams.setPageIndex(0);
        }
        if (!StringUtil.isNullOrEmpty(pageSize.toString())) {
            esParams.setPageSize(pageSize);
        } else {
            esParams.setPageIndex(1000);
        }
        JSONObject times = new JSONObject();
        if (!StringUtil.isNullOrEmpty(closeBeginDate) || !StringUtil.isNullOrEmpty(closeEndDate)) {
            JSONObject time = new JSONObject();
            String closeBeginDate1 = convertDateFormat(closeBeginDate);
            String closeEndDate1 = convertDateFormat(closeEndDate);
            time.put("gte", closeBeginDate1);
            time.put("lte", closeEndDate1);
            times.put("close_date", time);
        }
        if (!StringUtil.isNullOrEmpty(submitBeginDate) || !StringUtil.isNullOrEmpty(submitEndDate)) {
            JSONObject time = new JSONObject();
            String submitBeginDate1 = convertDateFormat(submitBeginDate);
            String submitEndDate1 = convertDateFormat(submitEndDate);
            time.put("gte", submitBeginDate1);
            time.put("lte", submitEndDate1);
            times.put("submit_date", time);
        }
        if (!StringUtil.isNullOrEmpty(finishBeginDate) || !StringUtil.isNullOrEmpty(finishEndDate)) {
            JSONObject time = new JSONObject();
            String finishBeginDate1 = convertDateFormat(finishBeginDate);
            String finishEndDate1 = convertDateFormat(finishEndDate);
            time.put("gte", finishBeginDate1);
            time.put("lte", finishEndDate1);
            times.put("end_date", time);

            //完成时间
        }
        JSONObject otherRanges = new JSONObject();
        if (!StringUtil.isNullOrEmpty(overDayCount)) {
            try {
                Calendar calendar = Calendar.getInstance();
                Calendar calendar2 = Calendar.getInstance();
                calendar.add(Calendar.DATE, -Integer.parseInt(overDayCount.split("-")[1])); //得到前n天
                calendar2.add(Calendar.DATE, -Integer.parseInt(overDayCount.split("-")[0]));
                Date date = calendar.getTime();
                Date date1 = calendar.getTime();
                DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                JSONObject time = new JSONObject();
                time.put("gte", df.format(date));
                time.put("lte", df.format(date1));
                times.put("promise_finish_date", time);
            } catch (Exception e) {
                return new ResultVO<>(StatusCode.NO_DATA, StatusCodeDesc.SUCCESS_DESC);
            }
            //otherRanges.put("over_day_count", overDayCount);
        }
        if (!StringUtil.isNullOrEmpty(answerDayCount)) {
            try {
                Calendar calendar = Calendar.getInstance();
                Calendar calendar2 = Calendar.getInstance();
                calendar.add(Calendar.DATE, -Integer.parseInt(answerDayCount.split("-")[1])); //得到前n天
                calendar2.add(Calendar.DATE, -Integer.parseInt(answerDayCount.split("-")[0]));
                Date date = calendar.getTime();
                Date date1 = calendar.getTime();
                DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                JSONObject time = new JSONObject();
                time.put("gte", df.format(date));
                time.put("lte", df.format(date1));
                times.put("reply_customer_date", time);
            } catch (Exception e) {
                return new ResultVO<>(StatusCode.NO_DATA, StatusCodeDesc.SUCCESS_DESC);
            }
            //otherRanges.put("answer_day_count", answerDayCount);
        }

        param.put("times", times);
//        param.put("otherRanges", otherRanges);


        JSONArray keywords = new JSONArray();
        keywords.add(quesDesc);
        keywords.add(contactPhoneORcontactName);
//        keywords.add(server);
        param.put("keywords", keywords);

        JSONArray keys = new JSONArray();
        keys.add("ques_desc");
        keys.add("contact_name");
        keys.add("contact_phone");

        param.put("keys", keys);
        esParams.setParams(param);
        esParams.setDatabase("taskpool");
        esParams.setTable("tbb_building_ques");
        esParams.setQueryType(2);

        System.out.println(esParams.toString());
        esUtil = ElasticSearchUtil.getInstance();
        Object value = esUtil.read(esParams);
        if (value != null) {
            JSONObject jsonResult = JSONObject.parseObject(value.toString());
            return new ResultVO<>(StatusCode.SUCCESS, StatusCodeDesc.SUCCESS_DESC, jsonResult);
        }
        return new ResultVO<>(StatusCode.NO_DATA, StatusCodeDesc.SUCCESS_DESC);
    }

    @PostMapping("/text")
    public ResultVO<JSONObject> text(String name, String age) throws IOException {
        JSONObject result = new JSONObject();
        result.put("name", name);
        result.put("age", age);
        if (result != null) {
            JSONObject jsonResult = JSONObject.parseObject(result.toString());
            return new ResultVO<>(StatusCode.SUCCESS, StatusCodeDesc.SUCCESS_DESC, jsonResult);
        }
        return new ResultVO<>(StatusCode.ERROR, StatusCodeDesc.ERROR_DESC);
    }

    //日期转换
    public String convertDateFormat(String date) {
        String time = date.split(" ")[0].split("-")[1].concat("/")
                .concat(date.split(" ")[0].split("-")[2])
                .concat("/").concat(date.split(" ")[0].split("-")[0]).concat(" ").concat(date.split(" ")[1]);
        return time;
    }

}
