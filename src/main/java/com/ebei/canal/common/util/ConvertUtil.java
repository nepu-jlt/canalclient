package com.ebei.canal.common.util;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @Description : 转换
 * @time 创建时间 : 2013-9-25
 * @author : shendx
 * @Copyright (c) 2013 一碑科技
 * @version
 */
public class ConvertUtil {

	/**
	 * 
	* Discription : 指定转换为<code>Integer</code>类型，当所处参数不为所转类型的时候，返回值为0
	* @param object 
	* @return
	* Integer
	* @throws     
	* @author : shendx
	 */
	public static Integer convertToInt(Object object) {
		String str = "0";
		if (object != null){
			str = object.toString();
		}
		BigDecimal bigDecimal = new BigDecimal(str);
		return bigDecimal.intValue();
	}
    
    /**
     * 
    * Discription :指定转换为<code>Double</code>类型，当所处参数不为所转类型的时候，返回值为0
    * @param object
    * @return
    * Double
    * @throws     
    * @author : shendx
     */
    public static Double convertToDouble(Object object)
    {
    	String str = "0";
		if (object != null && ValidateUtil.stringIsNotEmpty(object.toString())){
			str = object.toString();
		}
		BigDecimal bigDecimal = new BigDecimal(str);
		return bigDecimal.doubleValue();
    }
    
    /**
     * 
    * Discription :指定转换为<code>Double</code>类型，当所处参数不为所转类型的时候，返回值为0
    * @param object
    * @return
    * Double
    * @throws     
    * @author : shendx
     */
    public static Float convertToFloat(Object object)
    {
    	String str = "0";
		if (object != null){
			str = object.toString();
		}
		BigDecimal bigDecimal = new BigDecimal(str);
		return bigDecimal.floatValue();
    }
    
    /**
     * 
    * Discription :指定转换为<code>Long</code>类型，当所处参数不为所转类型的时候，返回值为0
    * @param object
    * @return
    * Double
    * @throws     
    * @author : shendx
     */
    public static Long convertToLong(Object object)
    {
    	String str = "0";
    	
    	if(object != null)
    	{   
    		if(ValidateUtil.stringIsNotEmpty(String.valueOf(object)))
    		{
    			str = String.valueOf(object);
    			str=replaceBlank(str);
    		}
    	}
    	
		BigDecimal bigDecimal = new BigDecimal(str);
		
		return bigDecimal.longValue();
    }
    
    
    /**
     * 替换字符串空格等
     * @param str
     * @return
     */
    public static String replaceBlank(String str) 
    {
        String dest = "";
        if (str!=null) 
        {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }
    
    /**
     * 
    * Discription :转换<code>Integer</code>类型，当所处值不为所转类型或者转换类型异常时
    * 			返回 defaultValue(默认值)值
    * @param obj
    * @param defaultValue
    * @return
    * Integer
    * @throws     
    * @author : shendx
     */
    public static Integer convert(Object obj,Integer defaultValue)
    {
    	Integer result = defaultValue;
    	if (ValidateUtil.isNull(obj))
    	{
    		return defaultValue;
    	}
    	try {
    		BigDecimal bigDecimal = new BigDecimal(obj.toString());
			result = bigDecimal.intValue();
		} catch (Exception e) {
			result = defaultValue;
		}
    	return result;
    }
    
    /**
     * 
    * Discription :转换<code>Long</code>类型，当所处值不为所转类型或者转换类型异常时
    * 			返回 defaultValue(默认值)值,其中Long用L表示 如1 就应该是1L
    * @param obj
    * @param defaultValue
    * @return
    * Integer
    * @throws     
    * @author : shendx
     */
    public static Long convert(Object obj,Long defaultValue)
    {
    	Long result = defaultValue;
    	if (ValidateUtil.isNull(obj))
    	{
    		return defaultValue;
    	}
    	try {
    		BigDecimal bigDecimal = new BigDecimal(obj.toString());
			result = bigDecimal.longValue();
		} catch (Exception e) {
			result = defaultValue;
		}
    	return result;
    }
    
    
    /**
     * 
    * Discription :转换<code>Float</code>类型，当所处值不为所转类型或者转换类型异常时
    * 			返回 defaultValue(默认值)值,其中Float用F表示 如1.0 就应该是1.0F
    * @param obj
    * @param defaultValue
    * @return
    * Integer
    * @throws     
    * @author : shendx
     */
    public static Float convert(Object obj,Float defaultValue)
    {
    	Float result = defaultValue;
    	if (ValidateUtil.isNull(obj))
    	{
    		return defaultValue;
    	}
    	try {
    		BigDecimal bigDecimal = new BigDecimal(obj.toString());
			result = bigDecimal.floatValue();
		} catch (Exception e) {
			result = defaultValue;
		}
    	return result;
    }
    
    /**
     * 
    * Discription :转换<code>Double</code>类型，当所处值不为所转类型或者转换类型异常时
    * 			返回 defaultValue(默认值)值,其中Double用D表示 如1.0 就应该是1.0D
    * @param obj
    * @param defaultValue
    * @return
    * Integer
    * @throws     
    * @author : shendx
     */
    public static Double convert(Object obj,Double defaultValue)
    {
    	Double result = defaultValue;
    	if (ValidateUtil.isNull(obj))
    	{
    		return defaultValue;
    	}
    	try {
    		BigDecimal bigDecimal = new BigDecimal(obj.toString());
			result = bigDecimal.doubleValue();
		} catch (Exception e) {
			result = defaultValue;
		}
    	return result;
    }
    
    /**
     * 
    * Discription :转换<code>String</code>类型，当所处值不为所转类型或者转换类型异常时
    * 			返回 defaultValue(默认值)值,其中Double用D表示 如1.0 就应该是1.0D
    * @param obj
    * @param defaultValue
    * @return
    * Integer
    * @throws     
    * @author : shendx
     */
    public static String convert(Object obj,String defaultValue)
    {
    	String result = defaultValue;
    	try {
    		result = obj.toString();
		} catch (Exception e) {
			result = defaultValue;
		}
    	return result;
    }
    
    /**
     * @description 
     * @author qsr
     * @time 2016年5月6日
     * @param o
     * @return
     */
    public static String nullToString(Object o){
		String result="";
		if(o!=null){
			result=String.valueOf(o);
		}
		return result;
	}
}
