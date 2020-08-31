package com.ebei.canal.common.util;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.MessageDigest;
import java.util.*;


public class EncryptUtils {
	public static final String PATTERN_ECB = "DES/ECB/PKCS5Padding";

    private static Key key =null;
    
    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static EncryptUtils encryptUtils = null;
    public static synchronized EncryptUtils getInstance() throws  Exception{
    	if(null==encryptUtils){
    		encryptUtils = new EncryptUtils();
    	}
    	return encryptUtils;
    }
    private EncryptUtils() throws  Exception {
    	 String desKey = "YC_3&0^!";
         DESKeySpec keySpec = new DESKeySpec(desKey.getBytes());//设置密钥参数
         SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");//获得密钥工厂
         key = keyFactory.generateSecret(keySpec);//得到密钥对象
    }

    public  String encode(String data) throws Exception {
        Cipher enCipher = Cipher.getInstance(PATTERN_ECB);//得到加密对象Cipher
        enCipher.init(Cipher.ENCRYPT_MODE,key);//设置工作模式为加密模式，给出密钥和向量
        byte[] pasByte = enCipher.doFinal(data.getBytes("utf-8"));
        BASE64Encoder base64Encoder = new BASE64Encoder();
        return base64Encoder.encode(pasByte);
    }

    public  String decode(String data) throws Exception{
        Cipher deCipher = Cipher.getInstance(PATTERN_ECB);
        deCipher.init(Cipher.DECRYPT_MODE,key);
        BASE64Decoder base64Decoder = new BASE64Decoder();
        byte[] pasByte=deCipher.doFinal(base64Decoder.decodeBuffer(data));
        return new String(pasByte,"UTF-8");
    }
    
    public String base64Encode(String data) throws Exception{
    	BASE64Encoder be = new BASE64Encoder();
    	return new String(be.encode(data.getBytes()));
    }
    
    /**
     * 
    * Discription : base64解密 当为密文MTIzNDU2Nzg=的时候，原有的decode函数代码无法解密
    * @param data
    * @return
    * @throws Exception
    * String
    * @throws     
    * @author : shendx
    * @date 2014-1-22 下午2:11:35
     */
    public String base64Decode(String data) throws Exception{
    	BASE64Decoder bn = new BASE64Decoder();
    	return new String(bn.decodeBuffer(data));
    }
    
    /**
     * 
    * Description : SHA1加密字符串
    * @param str
    * @return
    * String
    * @throws     
    * @author : fanghui
     */
	public static String SHA1(String str) {
		return encode("SHA1", str);
	}

	/**
	 * 
	* Description : SHA加密字符串
	* @param str
	* @return
	* String
	* @throws     
	* @author : fanghui
	 */
	public static String SHA(String str) {
		return encode("SHA", str);
	}

	/**
	 * 
	* Description : MD5加密字符串
	* @param str
	* @return
	* String
	* @throws     
	* @author : fanghui
	 */
	public static String MD5(String str) {
		return encode("MD5", str);
	}

	/**
	 * 
	* Description : 使用各种算法加密字符串,异常返回""
	* @param algorithm 算法
	* @param str 要加密的字符串
	* @return
	* String 加密后的字符串
	* @throws     
	* @author : fanghui
	 */
	public static String encode(String algorithm, String str) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
			messageDigest.update(str.getBytes());
			return getFormattedText(messageDigest.digest());
		} catch (Exception e) {
			e.printStackTrace() ;
			return "";
		}
	}
	
	/**
	 * 
	* Description : 字节数组转换为 十六进制 数
	* @param bytes
	* @return
	* String
	* @throws     
	* @author : fanghui
	 */
	private static String getFormattedText(byte[] bytes) {
		int len = bytes.length;
		StringBuilder buf = new StringBuilder(len * 2);
		// 把密文转换成十六进制的字符串形式
		for (int j = 0; j < len; j++) {
			buf.append(HEX_DIGITS[(bytes[j] >> 4) & 0x0f]);
			buf.append(HEX_DIGITS[bytes[j] & 0x0f]);
		}
		return buf.toString();
	}
	
	/**
	 * 根据排序完后的字符集进行md5加密
	 * @author wangpei 2017-8-1
	 * @param characterEncoding 字符集
	 * @param map 排序好的map字段
	 * @return
	 */
	public static String encodeMD5Sign(String characterEncoding,SortedMap<Object, Object> map){
		StringBuffer sb = new StringBuffer();
		Set es = map.entrySet();
		Iterator it = es.iterator();
		String projectTypeFilter = ConfigUtils.getConfigValue("conf/appconfig.properties", "projectTypeFilter");
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String k = (String) entry.getKey();
			Object v = entry.getValue();
			if(StringUtils.isNotEmpty(projectTypeFilter) && "on".equals(projectTypeFilter) && "projectType".equals(k) && (v==null || "".equals(v))) continue;
			if (null != v && !"".equals(v) && !"sign".equals(k) && !"key".equals(k)) {
				sb.append(k + "=" + v + "&");
			}
		}
		if(sb.length()>0)
        {
			sb.append("&");
        }
		sb.append("key=" + createSignKey());
		String sign = MD5Util.MD5Encode(sb.toString(), characterEncoding).toUpperCase();
		return sign;
	}
	
	/**
	 * 获取处理后的加密字符串key值
	 * @return 加密字符串key
	 * @author wangpei 2017-8-1
	 */
	public static String createSignKey()
	{
		String methodKey = ConfigUtils.getConfigValue("conf/appconfig.properties", "filterKey");
		StringBuffer str = new StringBuffer(methodKey);
		str = str.reverse();
		char[] chars = str.toString().toCharArray();
		Arrays.sort(chars);
		return new String(chars);
	}
	
	/**
     * 获取接口参数进行加密
     * 
     * @return String
     */
    public static String stitchingParameter(HttpServletRequest request) throws UnsupportedEncodingException
    {
		Map<String, Object> params = new HashMap<String, Object>();
    	String contentType = request.getHeader("Content-Type");
    	// 增加对json请求数据的支持
    	if (contentType !=  null && contentType.contains("json")) {
			StringBuffer jb = new StringBuffer();
			String line = null;
			try {
				BufferedReader reader = request.getReader();
				while ((line = reader.readLine()) != null)
					jb.append(line);
			} catch (Exception e) {
				throw new UnsupportedEncodingException();
			}
		
			try {
				JSONObject jsonObject =  JSONObject.parseObject(jb.toString());
				for (String key : jsonObject.keySet()) {
					params.put(key, jsonObject.get(key));
				}
			} catch (JSONException e) {
				throw new UnsupportedEncodingException();
			}
		} else {
			Enumeration<?> pNames = request.getParameterNames();
   
			String projectTypeFilter = ConfigUtils.getConfigValue("conf/appconfig.properties", "projectTypeFilter");
			while (pNames.hasMoreElements())
			{
				String pName = (String) pNames.nextElement();
				if("sign".equals(pName)||"_".equals(pName))continue;
				Object pValue = request.getParameter(pName);
				if(StringUtils.isNotEmpty(projectTypeFilter) && "on".equals(projectTypeFilter) && "projectType".equals(pName) && pValue==null) continue;
				params.put(pName, pValue);
			}
		}
        
        Set<String> keysSet = params.keySet();
        Object[] keys = keysSet.toArray();
        Arrays.sort(keys);
        StringBuffer temp = new StringBuffer();
        boolean first = true;
        for (Object key : keys) 
        {
            if (first) {
                first = false;
            } else {
                temp.append("&");
            }
            temp.append(key).append("=");
            Object value = params.get(key);
            String valueString = "";
            if (null != value) 
            {
                valueString = String.valueOf(value);
            }
            temp.append(valueString);
        }
        if(temp.length()>0)
        {
        	temp.append("&");
        }
        temp.append("key=" + createSignKey());

        return temp.toString();
    }
    
    /**
	 * 根据请求参数排序生成MD5加密字符串
	 * @author wangpei 2017-8-1
	 * @param characterEncoding 字符集
	 * @param request 请求信息
	 * @return
     * @throws UnsupportedEncodingException 
	 */
	public static String encodeMD5SignByRequest(String characterEncoding,HttpServletRequest request) throws UnsupportedEncodingException{
		String sign = MD5Util.MD5Encode(stitchingParameter(request), characterEncoding).toUpperCase();
		return sign;
	}

}
