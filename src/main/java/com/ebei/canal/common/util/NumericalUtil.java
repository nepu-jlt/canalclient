package com.ebei.canal.common.util;

import java.math.BigDecimal;

public class NumericalUtil {
	public static double dou(String dou1, String dou2){
		BigDecimal b1 = new BigDecimal(dou1);  
        BigDecimal b2 = new BigDecimal(dou2); 
   
        return b1.multiply(b2).doubleValue();
	}
	
	
	/**
	* Discription : 加法运算
	* @param v1
	* @param v2
	* @return
	* double
	* @date 2013-9-29 上午10:06:02 
	* @throws     
	* @author : shendx
	 */
	public static double add(double v1, double v2) { 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.add(b2).doubleValue(); 
	} 
	
	public static double add(double v1, double v2,int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		}
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.add(b2).setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue(); 
	}
	
	/**
	 * 
	* @see #add(double, double) 
	* @param v1
	* @param v2
	* @return
	* double
	* @date 2013-9-29 上午10:53:30 
	* @throws     
	* @author : shendx
	 */
	public static double add(String v1, String v2) { 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.add(b2).doubleValue(); 
	} 

	public static double add(String v1, String v2,int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		}
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.add(b2).setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue(); 
	}
	
	/**
	 * 
	* Discription : 减法
	* @param v1 被减数 
	* @param v2 减数
	* @return
	* double
	* @date 2013-9-29 上午10:06:43 
	* @throws     
	* @author : shendx
	 */
	public static double subtract(double v1, double v2) { 
		BigDecimal b1 = new BigDecimal(Double.toString(v1)); 
		BigDecimal b2 = new BigDecimal(Double.toString(v2)); 
		return b1.subtract(b2).doubleValue(); 
	} 
	
	/** 
	 * Description : 减法保留精度运算
	 * @param v1
	 * @param v2
	 * @param scale
	 * @return
	 * @date 2016年1月21日 下午2:55:48   
	 * @author : xiahao
	 */ 
	public static double subtract(double v1, double v2,int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		}
		BigDecimal b1 = new BigDecimal(Double.toString(v1)); 
		BigDecimal b2 = new BigDecimal(Double.toString(v2)); 
		return b1.subtract(b2).setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue(); 
	}
	
	/**
	 * 
	* @see #subtract(double, double)
	* @param v1 被减数 
	* @param v2 减数
	* @return
	* double
	* @date 2013-9-29 上午10:06:43 
	* @throws     
	* @author : shendx
	 */
	public static double subtract(String v1, String v2) { 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.subtract(b2).doubleValue(); 
	} 

	/**
	 * 
	* Discription : 乘法运算
	* @param v1
	* @param v2
	* @return
	* double
	* @date 2013-9-29 上午10:07:33 
	* @throws     
	* @author : shendx
	 */
	public static double multiply(double v1, double v2) { 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.multiply(b2).doubleValue(); 
	} 
	
	/**
	 * 
	* @see #multiply(double, double)
	**/
	public static double multiply(String v1, String v2) { 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.multiply(b2).doubleValue(); 
	} 

	/**
	 * 
	* Discription : 提供精确的乘法运算，并对运算结果截位，属于四舍五入
	* @param v1
	* @param v2
	* @param scale 保留小数的个数 不能小于0
	* @return
	* double
	* @date 2013-9-29 上午10:07:56 
	* @throws     
	* @author : shendx
	 */
	public static double multiply(double v1, double v2,int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.multiply(b2).setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue(); 
	} 
	
	/**
	 * 
	* @see #multiply(double, double, int) 
	* @param v1
	* @param v2
	* @param scale
	* @return
	* double
	* @date 2013-9-29 上午11:03:02 
	* @throws     
	* @author : shendx
	 */
	public static double multiply(String v1, String v2,int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.multiply(b2).setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue(); 
	} 


	/**
	 * 
	* Discription : 除法运算 
	* @param v1 被除数
	* @param v2 除数
	* @return
	* double
	* @date 2013-9-29 上午10:10:03 
	* @throws Exception 当小数点后面为无限循环的时候 会出现异常。需要保留小数    
	* @author : shendx
	 */
	public static double divide(double v1, double v2) throws Exception{ 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		if (v2 == 0){
			return 0.0;
		}
		return b1.divide(b2).doubleValue(); 
	} 
	
	/**
	 * 
	* @see #divide(double, double)
	 */
	public static double divide(String v1, String v2) throws Exception{ 
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		if ("0".equals(v2)){
			return 0.0;
		}
		return b1.divide(b2).doubleValue(); 
	} 

	/**
	 * 
	* Discription : 提供（相对）精确的除法运算. 
	* 				由scale参数指定精度，保留小数点后的数字为四舍五入. 
	* @param v1 被除数
	* @param v2 除数
	* @param scale 表示表示需要精确到小数点以后几位 ,不能小于0
	* @return
	* double
	* @date 2013-9-29 上午10:10:57 
	* @throws     
	* @author : shendx
	 */
	public static double divide(double v1, double v2, int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
	
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		if (v2 == 0){
			return 0.0;
		}
		try {
			return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
		} catch (Exception e) {
			return b1.divide(b2, scale, BigDecimal.ROUND_HALF_EVEN).doubleValue();
		} 
	} 
	
	/**
	 * @see #divide(double, double, int)
	 */
	public static double divide(String v1, String v2, int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
		if ("0".equals(v2)){
			return 0.0;
		}
		BigDecimal b1 = new BigDecimal(v1); 
		BigDecimal b2 = new BigDecimal(v2); 
		return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue(); 
	} 


	/**
	 * 
	* Discription : 提供精确的小数位四舍五入处理. 
	* @param v 需要四舍五入的数字 
	* @param scale 小数点后保留几位 
	* @return
	* double
	* @date 2013-9-29 上午10:11:51 
	* @throws     
	* @author : shendx
	 */
	public static double round(double v, int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
		BigDecimal b = new BigDecimal(v); 
		return b.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue(); 
	} 
	
	/**
	 * @see #round(double, int)
	 */
	public static double round(String v, int scale) { 
		if (scale < 0) { 
			throw new IllegalArgumentException("The scale must be a positive integer or zero"); 
		} 
		BigDecimal b = new BigDecimal(v); 
		return b.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue(); 
	} 
}
