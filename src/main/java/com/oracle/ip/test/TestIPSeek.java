package com.oracle.ip.test;

import java.net.URLDecoder;

import com.oracle.ip.utils.IPSeekerExt;
import com.oracle.ip.utils.IPSeekerExt.RegionInfo;

public class TestIPSeek {

	/*124.205.0.77	[30/May/2013:18:54:28	+0800]	/forum.php	HTTP/1.1"	71428
	49.89.92.94	[30/May/2013:18:54:27	+0800]	/home.php?mod=spacecp&ac=pm&op=checknewpm&rand=1369911265	HTTP/1.1"	-
	123.125.71.78	[30/May/2013:18:54:28	+0800]	/forum.php	HTTP/1.1"	76440
	125.37.97.140	[30/May/2013:18:54:29	+0800]	/home.php?mod=spacecp&ac=follow&op=checkfeed&rand=1369911267	HTTP/1.1"	-
	124.205.0.77	[30/May/2013:18:54:29	+0800]	/home.php?mod=spacecp&ac=follow&op=checkfeed&rand=1369911268	HTTP/1.1"	-
	124.205.0.77	[30/May/2013:18:54:29	+0800]	/home.php?mod=misc&ac=sendmail&rand=1369911268	HTTP/1.1"	-
	118.123.249.72	[30/May/2013:18:54:29	+0800]	/api.php?mod=js&bid=94	HTTP/1.1"	275
	182.148.111.6*/
	
	
	public static void main(String[] args) {
		IPSeekerExt ipSeekerExt = new IPSeekerExt();
		RegionInfo info = ipSeekerExt.analyticIp("124.205.0.77");
		System.out.println(info.getCity()+" "+info.getCountry()+" "+info.getProvince());
		System.out.println("");
		//p_url=http://172.16.0.150:8080/BIG_DATA_LOG2/demo.js
	//	String url=URLDecoder.decode("/HHUspringmvc/aa.xhtml?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2FBIG_DATA_LOG2%2Fdemo4.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A24&ver=1&pl=website&sdk=js&u_ud=550DE3C3-3923-4C36-BDBD-45784A6E314E&u_mid=yuhui&u_sd=80309241-572D-496E-92B1-884F411ACDBE&c_time=1497831960366&b_iev=Mozilla%2F5.0%20(compatible%3B%20MSIE%2010.0%3B%20Windows%20NT%206.1%3B%20WOW64%3B%20Trident%2F7.0%3B%20LCTE)&b_rst=1366*768");
      // System.out.println(url);
	}

}
