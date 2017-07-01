package com.bf.log.mapper;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.decommission_jsp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bf.log.constants.LogConstants;
import com.bf.log.constants.ToDate;
import com.bf.log.dimention.DregionDimention;
import com.oracle.ip.utils.IPSeekerExt;
import com.oracle.ip.utils.IPSeekerExt.RegionInfo;

public class DregionMapper extends Mapper<LongWritable, Text, DregionDimention, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, DregionDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String[] values=value.toString().split("\t");
		String logDate = ToDate.toHouse(values[1]);
		String logIP=values[0];
		String[] logUrl=values[3].split("&");
		
		
		
		////////////////////////////////地域信息分析:以ip为key求个数//////////////////////
		if(logIP!=null || logIP!=""){
			DregionDimention dregionDimention=new DregionDimention();//初始化
			
			dregionDimention.setLogIp(logIP);
			dregionDimention.setLogType(LogConstants.Dregion);
			context.write(dregionDimention, new LongWritable(1));
			
			IPSeekerExt ipSeekerExt = new IPSeekerExt();
			RegionInfo info = ipSeekerExt.analyticIp(logIP);
			for(String vv:logUrl){
				if(vv.contains("u_sd")){
					if(info.getCountry().equals("unknown")){
						break;
					}
					dregionDimention.setLogIp(info.getCountry()+"\t"+info.getProvince());
					dregionDimention.setLogUSD(vv);
					dregionDimention.setLogDate(logDate);
					dregionDimention.setLogType(LogConstants.SessionNum);
					//添加会话个数
					context.write(dregionDimention, new LongWritable(1));
					//跳出个数
					dregionDimention.setLogType(LogConstants.OutNum);
					context.write(dregionDimention, new LongWritable(1));
					
					break;
				}
			}
		}
		
		/////////////////////////////用户访问深度分析/////////////////////////////////////
		for(String valu:logUrl){
			
			if(valu.startsWith("u_ud")){
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogUUD(valu);
				for(String val:logUrl){
					if(val.contains("p_url")){
						dregionDimention.setLogPURL(val);
						dregionDimention.setLogType(LogConstants.Userdepth);//深度分析
						context.write(dregionDimention, new LongWritable(1));
						break;
					}
				}
				break;
			}
		}
		
		/////////////////////////////6、外链数据分析////////////////////////
		
		for(String keys:logUrl){
			if(keys.startsWith("p_ref")&&!keys.contains("BIG_DATA_LOG2")){
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogPREF(keys.split("=")[1]);
				dregionDimention.setLogType(LogConstants.OutChain);
				context.write(dregionDimention, new LongWritable(1));
				
			}
		}
		
		////////////////////////外链会话(跳出率)分析//////////////////////////
		for(String kkey:logUrl){
			if(kkey.startsWith("p_ref")&&!kkey.contains("BIG_DATA_LOG2")){
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogPREF(kkey.split("=")[1]);
				dregionDimention.setLogType(LogConstants.OutChainNum);
				context.write(dregionDimention, new LongWritable(1));
				
			}
		}
		
		
		
		///////////////////////////7、订单数据分析模块//////////////
		
		for(String keys:logUrl){
			//订单支付成功个数
			if(keys.equals("en=e_cs")){
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogEN("en=e_cs");
				dregionDimention.setLogType(LogConstants.OrderSuccess);
				context.write(dregionDimention, new LongWritable(1));
			}
			//订单退款个数
			else if(keys.equals("en=e_cr")){
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogEN("en=e_cr");
				dregionDimention.setLogType(LogConstants.OrderRetreat);
				context.write(dregionDimention, new LongWritable(1));
			}
		}
		
		/////////////////////////订单个数，订单金额和////////////////////////////
		
		
		for(String keys:logUrl){
			if(keys.contains("oid")){
				String[] oid=keys.split("=");
				DregionDimention dregionDimention=new DregionDimention();
				dregionDimention.setLogDate(logDate);
				dregionDimention.setLogOID(oid[1]);
				dregionDimention.setLogType(LogConstants.OrderMoney);
				
				Double cua=0.0;
				for(String keyss:logUrl){
					if(keyss.contains("cua")){
						String[] money=keyss.split("=");
						cua=Double.valueOf(money[1]) ;
						context.write(dregionDimention, new LongWritable(Integer.parseInt(new java.text.DecimalFormat("0").format(cua))));
						break;
					}
				}
				
				break;
			}
		}
		
		
		
		
	}
}
