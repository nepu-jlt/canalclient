package com.ebei.canal.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * 读取配置文件工具类
 * 
 * @author 姓名 工号
 * @version [版本号, 2016年9月14日]
 */
public class ConfigUtils
{
    private static Map<String, String> config = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);
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
        Properties props = null;
        // 优先从map中读取配置
        if (config.containsKey(propertiesName + key)) {
            return config.get(propertiesName + key);
        }
        // 再从外部文件读取配置
        FileInputStream fis = null;
        try {
            String proFilePath = System.getProperty("user.dir") + File.separator + propertiesName;
            fis = new FileInputStream(proFilePath);
            props = new Properties();
            props.load(fis);
    
            if (StringUtils.isNotEmpty(props.getProperty(key)))
            {
                resultValue = props.getProperty(key).trim();
                config.put(propertiesName + key, resultValue);
            }
        } catch (Exception e) {
            logger.warn("No config file found! Use default value of key: " + key);
            try {
                props = PropertiesLoaderUtils.loadAllProperties(propertiesName);
        
                if (StringUtils.isNotEmpty(props.getProperty(key))) {
                    resultValue = props.getProperty(key).trim();
                }
            } catch (Exception ee) {
                logger.error("Default config read error");
            }
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
