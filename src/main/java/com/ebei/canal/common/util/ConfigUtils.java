package com.ebei.canal.common.util;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * 读取配置文件工具类
 * 
 * @author 姓名 工号
 * @version [版本号, 2016年9月14日]
 */
public class ConfigUtils
{
    /**
     * 根据key读取配置项
     * 
     * @param propertiesName
     * @param key
     * @return
     */
    public static String getConfigValue(String propertiesName, String key)
    {
        String resultValue = "";
        try
        {
            Properties props = PropertiesLoaderUtils.loadAllProperties(propertiesName);
            
            if (StringUtils.isNotEmpty(props.getProperty(key)))
            {
                resultValue = props.getProperty(key).trim();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
        return resultValue;
    }
    
    /**
     * 读取配置文件的map
     * 
     * @return
     */
    public static Map<String, String> readProperties(String fileName)
    {
        // Resource resource = new ClassPathResource("appconfig.properties");
        // Properties props = PropertiesLoaderUtils.loadProperties(resource);
        Properties props;
        Map<String, String> propertiesMap = new HashMap<String, String>();
        try
        {
            props = PropertiesLoaderUtils.loadAllProperties(fileName);
            Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
            while (it.hasNext())
            {
                Entry<Object, Object> entry = it.next();
                String key = (String)entry.getKey();
                String value = (String)entry.getValue();
                propertiesMap.put(key, value);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        return propertiesMap;
    }
}
