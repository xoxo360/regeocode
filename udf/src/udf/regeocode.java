package udf;

//jdk自带的jar包    
import java.io.BufferedReader;  
import java.io.IOException;  
import java.io.InputStreamReader;  
import java.net.MalformedURLException;  
import java.net.URL;  
import java.net.URLConnection;  
import org.json.JSONException;
import org.json.JSONObject;
//hivejar包hive-exec-1.2.1.jar
import org.apache.hadoop.hive.ql.exec.UDF;

public class regeocode extends UDF {
		//个人申请的密钥
		private static String app_token = "268d99808083b0c30b93b952e329d869"; 
		//解析gps返回数据
		public static String getinfo(String gps,String gpstype)throws JSONException{ 
			//判断传入的输出gps类型是否正确
			if ((!gpstype.equals("regeocode")) && (!gpstype.equals("country")) && (!gpstype.equals("province")) && (!gpstype.equals("city")) && (!gpstype.equals("district")) && (!gpstype.equals("adcode")) && (!gpstype.equals("township")) && (!gpstype.equals("street")) && (!gpstype.equals("number")) && (!gpstype.equals("formatted_address")))
		    {
		      return "Unknown Args";
		    }
			//判断传入gps数据是否存在
			if ((gps == null)||(gps == "")) { 
				return "";
			}
			
			//String gps = "经度:121.360357,纬度:31.221806";
			//规范化经纬度数据，正则去中文和冒号
			String reg = "[\u4e00-\u9fa5]+:";
	    		String location = gps.replaceAll(reg, "");
			
	        //输入经纬度，从接口获取json代码，输入格式格式是 经度,纬度  
	        String ApiUrl = "http://restapi.amap.com/v3/geocode/regeo?key="+app_token+"&location="+location;  //接口网址
	        String ApiResult = getResponse(ApiUrl);  //高德接口返回的是JSON格式的字符串  
	        //获取接口返回的json
	        new JSONObject();
			JSONObject jo = new JSONObject(ApiResult);  
			//获取接口获取状态
			String status = jo.getString("status").toString();
			//判断接口返回结果是否成功
			if (status.equals("0")){
				return "failed";
			}
			//截取行政区域json	 
			JSONObject addressComponent =  jo.getJSONObject("regeocode").getJSONObject("addressComponent");
			
			if (gpstype.equals("regeocode")){
				 //全部json代码 
				String info =jo.getJSONObject("regeocode").toString();
				return info;
				}
			
			if (gpstype.equals("country")){
				//国家
		        String country=addressComponent.get("country").toString();
				return country;
				}
			if (gpstype.equals("province")){
				//省份
		        String province=addressComponent.get("province").toString();
				return province;
				}
			if (gpstype.equals("city")){
		        //城市
		        String city=addressComponent.get("city").toString();
				return city;
				}
			if (gpstype.equals("district")){
		        //城区
		        String district=addressComponent.get("district").toString();
				return district;
				}
			if (gpstype.equals("adcode")){
		        //城区代号
		        String adcode=addressComponent.get("adcode").toString();
				return adcode;
				}
			if (gpstype.equals("township")){
		        //乡镇
		        String township=addressComponent.get("township").toString();
				return township;
				}
			if (gpstype.equals("street")){
		        //街道名称
		        String street=addressComponent.getJSONObject("streetNumber").get("street").toString();
				return street;
				}
			if (gpstype.equals("number")){
		        //门牌号
		        String number=addressComponent.getJSONObject("streetNumber").get("number").toString();
				return number;
				}
			if (gpstype.equals("formatted_address")){
		        //结构化地址信息
		        String formatted_address=jo.getJSONObject("regeocode").get("formatted_address").toString();
				return formatted_address;
				}
			//异常处理
	        String error="Error";
			return error;
			}
		
		//接口请求发送
		 private static String getResponse(String ApiUrl){  
		        //用JAVA发起http请求，并返回json格式的结果  
		        StringBuffer result = new StringBuffer();  
		        try {  
		            URL url = new URL(ApiUrl);  
		            URLConnection conn = url.openConnection();  
		            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));  
		            String line;  
		            while((line = in.readLine()) != null){  
		                result.append(line);  
		            }  
		            in.close();  
		  
		        } catch (MalformedURLException e) {  
		            e.printStackTrace();  
		        } catch (IOException e) {  
		            e.printStackTrace();  
		        }  
		        return result.toString();  
		    }  
		 //udf必须要求重写evaluate函数，获取返回的regeocode数据；
		 public static String evaluate(String gps,String gpstype){
			 String info = "";
			try {
				info = getinfo(gps,gpstype);
			} catch (JSONException e) {
				e.printStackTrace();
			} 
			 return info;
		 }
		 
		 //测试
		
		 //public static void main(String[] args) {  
				//gps 参数
		//		String gps = "121.321030,31.192663";	
				//查询类型
		//		String gpstype = "street";
		    		//调用函数接口获取高德逆api返回的数据
		//		String info = null;
		//		try {
		//			info = getinfo(gps,gpstype);
		//		} catch (JSONException e) {
		//			e.printStackTrace();
		//		} 
		//       System.out.println(info); 
		//}  
}
