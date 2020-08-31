package com.ebei.canal.common.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Pattern;


/**
 * 
 * @Description :验证类 
 * @time 创建时间 : 2013-8-8
 * @author : dison
 * @Copyright (c) 2013 一碑科技
 * @version
 */
public class ValidateUtil {

	/**
	 * 
	* Discription :判断字符是否不为空 
	* @param str
	* @return
	* boolean
	* @throws     
	* @author : dison
	 */
	public static boolean stringIsNotEmpty(String str)
	{
		return (null != str && str.trim().length() >0) ? true : false;
	}
	
	/**
	 * 
	* Discription :判断字符是否为空 
	* @param str
	* @return
	* boolean
	* @throws     
	* @author : dison
	 */
	public static boolean stringIsEmpty(String str)
	{
		return !stringIsNotEmpty(str);
	}
	
	/**
	 * 
	* Discription :判断集合是否不为空 
	* @param con
	* @return
	* boolean
	* @throws     
	* @author : dison
	 */
	public static boolean collectionsIsNotEmpty(Collection<?> con)
	{
		return (null != con && !con.isEmpty()) ? true : false;
	}
	
	/**
	 * 
	* Discription :  判断集合是否为空
	* @param con
	* @return
	* boolean
	* @throws     
	* @author : dison
	 */
	public static boolean collectionsIsEmpty(Collection<?> con)
	{
		return !collectionsIsNotEmpty(con);
	}
	
	
	/**
	 * 如果值为空，用-1表示（全部）
	 * @param s
	 * @return
	 */
	public static String getNotStringValue(String s)
	{
		return stringIsEmpty(s) ? "-1" : s;
	}
	
	/**
	 * 
	* Discription :isObjEmpty 检测对象是否为空
	* @return 检查结果 ：<b>true</b> 对象为空 
	* 				   <b>false</b> 对象不为空
	* @throws     
	* @author : dison
	 */
	public static boolean isObjEmpty(Object object)
	{
		return object == null ? true : false;
	}
	
	/**
	 * 
	* Discription : isMapEmpty判断map是否为空
	* @return <b>true</b>表示为空 <b>false</b>表示不为空
	* @throws     
	* @author : dison
	 */
	public static boolean isMapEmpty (Map<?, ?> map)
	{
		return (null == map || map.isEmpty()) ? true : false;
	}
	
	/**
	 * 
	* Discription : isMapNotEmpty判断map是否不为空
	* @return <b>true</b>表示不为空 <b>false</b>表示为空
	* @throws     
	* @author : dison
	 */
	public static boolean isMapNotEmpty (Map<?, ?> map)
	{
		return !isMapEmpty(map);
	}
	
	/**
	 * 
	* Discription : isNull判断obj是否为空.此方法在效率上稍慢,因为其是采用的 try catch 来一一过滤!<br>
	* 				其原理是用Method 判断传入的object值中的类路径，以及object下属所使用的方法，通过getMethod<br>
	* 				实现
	* @param obj
	* @return <b>true</b> 为空 <b>false</b>不为空
	* @throws     
	* @author : dison
	 */
    public static boolean isNull (Object obj) {  
        // 判断对象是否为空  
        if (obj == null || "".equals(obj)) {  
            return true;  
        }
        // 通过类路径判断是否是String类型
        else if ("java.lang.String".equals(obj.getClass().getName())){  
            // 替换各种空格  
            String tmobj = Pattern.compile("//r|//n|//u3000").matcher((String)obj).replaceAll("");  
            // 匹配空  
            return Pattern.compile("^(//s)*$") .matcher(tmobj).matches();  
        } 
        else {  
            // 方法类  
            Method method = null;  
            String newInput = "";  
            try {  
                // 判断obj类中是否有size函数
                method = obj.getClass().getMethod("size"); 
                //等同调用obj中的size函数(下同)
                newInput = String.valueOf(method.invoke(obj));  
                // size为0的场合  
                if (Integer.parseInt(newInput) == 0) {  
  
                    return true;  
                } else {  
  
                    return false;  
                }  
            } catch (Exception e) {  
                // 访问失败  
                try {  
                    method = obj.getClass().getMethod("getItemCount");  
                    newInput = String.valueOf(method.invoke(obj));  
                    if (Integer.parseInt(newInput) == 0) {  
  
                        return true;  
                    } else {  
  
                        return false;  
                    }  
                } catch (Exception ex) {  
                    // 访问失败  
                    try{  
                        // 判断传入参数的长度  
  
                        // 长度为0的场合  
                        if (Array.getLength(obj) == 0) {  
  
                            return true;  
                        } else {  
  
                            return false;  
                        }  
                    } catch (Exception exx) {  
                        // 访问失败  
                        try{  
                            method = Iterator.class.getMethod("hasNext");  
                            newInput = String.valueOf(method.invoke(obj));  
                            if (!Boolean.valueOf(newInput)) {  
                                return true;  
                            } else {  
                                return false;  
                            }  
                              
                        } catch (Exception exxx) {  
                            // 以上场合不满足  
                            return false;  
                        }  
                    }  
                }  
            }  
        }  
    } 
    
    
    /**
     * 判断对象不为空
     * 
     * @param value
     * @return
     */
    public static Boolean isNotNull(Object value)
    {
        if (value != null)
        {
            return true;
        }
        
        return false;
    }
    
    
    /**
     * 
    * Discription : 判断bean中是否有存在Set集合。因在bean转json，在bean中有相关的Set集合，
    *  	            导致无法正常转换为json对象，此方法是bean中是否有存在set集合
    * @param bean
    * @return
    * boolean
    * @throws     
    * @author : dison
     */
    public static boolean isObjectContainSet(Object bean)
    {
    	boolean flag = false;
    	try {
			PropertyDescriptor[] props = Introspector.getBeanInfo(bean.getClass(), Object.class)     
			        .getPropertyDescriptors();
			for (int i = 0; i < props.length; i++) {
				String className = props[i].getPropertyType().getName(); //获取类路径
				if ("java.util.Set".equals(className))
				{
					flag = true;
					break;
				}
			}
		} catch (IntrospectionException e) {
			flag = false;
			e.printStackTrace();
		} 
    	
    	return flag;
    }
    
    /**
     * 
    * Discription : 将空值转换为空字符串
    * @param o
    * @return
    * String
    * @throws     
    * @author : Zhough
     */
    public static String nullParseToString(Object o){
    	String result = "";
		if(o != null){
			result = String.valueOf(o);
		}
		return result;
    }
    
    /**
     * 
    * Discription :判断字符串中是否含有数字 
    * @param str
    * @return
    * boolean true 包含 false 不包含
    * @throws     
    * @author : shendx
    * @date 2013-11-28 下午3:39:03
     */
	public static boolean isDigit(String str) {
		boolean isDigit = false;
		if (stringIsNotEmpty(str))
			return isDigit;
		for (int i = 0; i < str.length(); i++) { // 循环遍历字符串
			if (Character.isDigit(str.charAt(i))) { // 用char包装类中的判断数字的方法判断每一个字符
				isDigit = true;
				break;
			}
		}
		return isDigit;
	}
	
	public static String parseNullToDefault(String str,String defaultValue){
		String result = defaultValue;
		if(str != null){
			result = String.valueOf(str);
		}
		return result;
	}
	
	/**
	 * 
	* Discription : 实体集合某个字段值的拼接，con集合，separator分隔符，param字段
	* @param con
	* @param separator
	* @param param
	* @return
	* String   
	* @author : han
	 */
	public static <T> String getStringForCollectionBySeparator(Collection<T> con,String separator,String param){
		String result = "";
		try{
			String firstLetter = param.substring(0, 1).toUpperCase();    
	        String getter = "get" + firstLetter + param.substring(1);  
	        if(ValidateUtil.collectionsIsNotEmpty(con)){
	        	Iterator<T> it = con.iterator();
				while(it.hasNext()){
					T entity = it.next();
					Method method = entity.getClass().getMethod(getter, new Class[] {}); 
					Object value = method.invoke(entity, new Object[] {});
					result += value + separator;
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		if(ValidateUtil.stringIsNotEmpty(result)){
			result = result.substring(0, result.length()-1);
		}
		return result;
	}
	
	/**
	 * 
	* Discription : 将字符串按照分隔符转换成集合
	* @param param
	* @param separator
	* @return
	* List<String>
	* @throws     
	* @author : han
	 */
	public static List<String> getCollectionForStringBySeparator(String param,String separator){
		List<String> col = new ArrayList<String>();
		
		if(ValidateUtil.stringIsEmpty(param)){
			return col;
		}
		String[] array = param.split(separator);
		for(String it : array){
			if(ValidateUtil.stringIsNotEmpty(it))
			col.add(it);
		}
		return col;
	}
	
	/**
	 * 
	* Discription :判断是否为数字类型(非浮点类型) 
	* @param str
	* @return
	* boolean
	* @date 2015-11-11 下午5:49:47 
	* @throws     
	* @author :  dison
	 */
	public static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]+");
		return pattern.matcher(str).matches();
	}
}
