package com.ebei.canal.common.util;

import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

/**
 * 远程类访问util
 * 
 * @author  wang,pei
 * @version  [版本号, 2015年9月8日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public class WebUtils
{
    /**
     * 访问请求
     * 
     * @param httpUrl
     * @return
     */
    public static String getHTML(String httpUrl)
    {
        String result = "";
        URLConnection urlCon = null;
        try
        {
            URL url = new URL(httpUrl);
            StringBuffer document = new StringBuffer();
            BufferedReader reader = null;
            try
            {
                urlCon = (HttpURLConnection)url.openConnection();
                urlCon.setConnectTimeout(2000);
                urlCon.setReadTimeout(5000);
                reader = new BufferedReader(new InputStreamReader(urlCon.getInputStream(), "utf-8"));
                String Result = "";
                while ((Result = reader.readLine()) != null)
                {
                    document.append(Result);
                }
                
                result = document.toString();
            }
            catch (IOException e)
            {
                result = "服务未响应";
            }
            finally
            {
                if (reader != null)
                {
                    try
                    {
                        reader.close();
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (MalformedURLException e)
        {
            result = "不支持的协议";
        }
        
        return result;
    }
    
    /**
     * 获取本地ip
     * 
     * @return
     */
    public static String getLocalHostIp()
    {
        String ip = null;
        InetAddress addr;
        try
        {
            addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress().toString();
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
        
        return ip;
    }
    
    /**
     * 获取ip
     * 
     * @param request
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getClientIp(HttpServletRequest request)
    {
        String ip = request.getHeader("x-forwarded-for");
        
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip))
        {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip))
        {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip))
        {
            ip = request.getRemoteAddr();
        }
        
        return ip;
    }
    
    public static String getClientRealIp(HttpServletRequest request)
    {
    	String ip = request.getHeader("X-Forwarded-For");
        if(StringUtils.isNotEmpty(ip) && !"unKnown".equalsIgnoreCase(ip)){
            //多次反向代理后会有多个ip值，第一个ip才是真实ip
            int index = ip.indexOf(",");
            if(index != -1){
                return ip.substring(0,index);
            }else{
                return ip;
            }
        }
        ip = request.getHeader("X-Real-IP");
        if(StringUtils.isNotEmpty(ip) && !"unKnown".equalsIgnoreCase(ip)){
            return ip;
        }
        return request.getRemoteAddr();
    }
    
    /**
     * 获取浏览器类型
     * 
     * @param agent
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getBrowserName(String agent) 
    {
     if(agent.indexOf("msie 7")>0){
      return "ie7";
     }else if(agent.indexOf("msie 8")>0){
      return "ie8";
     }else if(agent.indexOf("msie 9")>0){
      return "ie9";
     }else if(agent.indexOf("msie 10")>0){
      return "ie10";
     }else if(agent.indexOf("msie")>0){
      return "ie";
     }else if(agent.indexOf("opera")>0){
      return "opera";
     }else if(agent.indexOf("opera")>0){
      return "opera";
     }else if(agent.indexOf("firefox")>0){
      return "firefox";
     }else if(agent.indexOf("webkit")>0){
      return "webkit";
     }else if(agent.indexOf("gecko")>0 && agent.indexOf("rv:11")>0){
      return "ie11";
     }else{
      return "Others";
     }
    }
}
