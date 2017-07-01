package com.bf.log.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.log.connection.JdbcManager;
import com.bf.log.constants.LogConstants;
import com.bf.log.dimention.DregionDimention;

public class DregionFormat extends OutputFormat<DregionDimention, LongWritable> {

	@Override
	public RecordWriter<DregionDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		Connection connection = JdbcManager.getConnection(conf);

		return new MyUserRecordWriter(conf, connection);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

	class MyUserRecordWriter extends RecordWriter<DregionDimention, LongWritable> {
		private Configuration conf;
		private Connection con;

		public MyUserRecordWriter(Configuration conf, Connection connection) {
			// TODO Auto-generated constructor stub
			this.conf = conf;
			this.con = connection;
		}

		HashMap<String, Long> hashDregionNumber = new HashMap<String, Long>();
		private HashMap<String, PreparedStatement> psMaps = new HashMap<String, PreparedStatement>();
		HashMap<String, Long> hashSessionNum = new HashMap<String, Long>();
		HashMap<String, Long> hashSessionOutNum = new HashMap<String, Long>();
		HashMap<String, Long> hashDepthNum = new HashMap<String, Long>();
		HashMap<String, Long> hashOutChainNum = new HashMap<String, Long>();
		HashMap<String, Long> hashOutChainRateNum = new HashMap<String, Long>();
		HashMap<String, Long> hashOrderSuccessNum = new HashMap<String, Long>();
		HashMap<String, Long> hashOrderRetreatNum = new HashMap<String, Long>();
		HashMap<String, Long> hashOrderMoney = new HashMap<String, Long>();
		HashMap<String, Integer> hashOrderNumber = new HashMap<String, Integer>();
		@Override
		public void write(DregionDimention key, LongWritable value) throws IOException, InterruptedException {
			if(key.getLogType().equals("Dregion")){
				hashDregionNumber.put(key.getLogIp(), value.get());
			}
			else if(key.getLogType().equals(LogConstants.SessionNum)){
				long count=(hashSessionNum.get(key.getLogDate()+"\t"+key.getLogIp())==null?0:hashSessionNum.get(key.getLogDate()+"\t"+key.getLogIp()))+1;
				hashSessionNum.put(key.getLogDate()+"\t"+key.getLogIp(), count);
			}
			else if(key.getLogType().equals(LogConstants.OutNum)){
				long count=(hashSessionOutNum.get(key.getLogDate()+"\t"+key.getLogIp())==null?0:hashSessionOutNum.get(key.getLogDate()+"\t"+key.getLogIp()))+value.get();
				hashSessionOutNum.put(key.getLogDate()+"\t"+key.getLogIp(), count);
			}
			else if(key.getLogType().equals(LogConstants.Userdepth)){
				long count=(hashDepthNum.get(key.getLogDate()+"\t"+key.getLogUUD())==null?0:hashDepthNum.get(key.getLogDate()+"\t"+key.getLogUUD()))+1;
				//System.out.println("pp:"+key.toString()+"\t"+count);
				hashDepthNum.put(key.getLogDate()+"\t"+key.getLogUUD(), count);
			}
			else if(key.getLogType().equals(LogConstants.OutChain)){
				hashOutChainNum.put(key.getLogDate()+"\t"+key.getLogPREF(), value.get());
			}
			else if(key.getLogType().equals(LogConstants.OutChainNum)){
				hashOutChainRateNum.put(key.getLogDate()+"\t"+key.getLogPREF(), value.get());
			}
			else if(key.getLogType().equals(LogConstants.OrderSuccess)){
				hashOrderSuccessNum.put(key.getLogDate(), value.get());
			}else if(key.getLogType().equals(LogConstants.OrderRetreat)){
				hashOrderRetreatNum.put(key.getLogDate(), value.get());
			}else if(key.getLogType().equals(LogConstants.OrderMoney)){
				hashOrderMoney.put(key.getLogDate()+"\t"+key.getLogOID(), value.get());
				int order=(int) ((hashOrderNumber.get(key.getLogDate())==null?0:hashOrderNumber.get(key.getLogDate()))+1);
				hashOrderNumber.put(key.getLogDate(), order);
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			////////////////////////////// 计算ip个数/////////////////////////
			PreparedStatement ps = getPSSQL("dregion_Ipnumber");

			for (String entry : hashDregionNumber.keySet()) {
				try {
					ps.setString(1, entry);
					ps.setString(2, String.valueOf(hashDregionNumber.get(entry)));
					ps.setString(3, String.valueOf(hashDregionNumber.get(entry)));

					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			
			///////////////////////////////////////////会话个数/会话个数及跳出率/////////////////////////////
			ps=getPSSQL("dregion_sessionNum");
			for (String entry : hashSessionNum.keySet()) {
				//System.out.println(entry+"\t"+hashSessionNum.get(entry));
				//System.out.println("out:"+entry+"\t"+hashSessionOutNum.get(entry));
				String[] val= entry.split("\t");
				try {
					ps.setString(1,val[0] );
					ps.setString(2, val[1]);
					ps.setString(3, val[2]);
					ps.setString(4, String.valueOf(hashSessionNum.get(entry)) );
					ps.setString(5, String.valueOf(hashSessionOutNum.get(entry)));
					ps.setString(6, String.valueOf(hashSessionOutNum.get(entry)/hashSessionNum.get(entry)));
					
					ps.setString(7, String.valueOf(hashSessionNum.get(entry)) );
					ps.setString(8, String.valueOf(hashSessionOutNum.get(entry)));
					ps.setString(9, String.valueOf(hashSessionOutNum.get(entry)/hashSessionNum.get(entry)));
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			/////////////////////////用户访问深度分析///////////////////////////////////
			ps=getPSSQL("depth_num");
			for (String entr:hashDepthNum.keySet()) {
				//System.out.println(entr+"\t"+hashDepthNum.get(entr));
				String[] val=entr.split("\t");
				try {
					ps.setString(1,val[0]);
					ps.setString(2, val[1]);
					ps.setString(3, String.valueOf(hashDepthNum.get(entr)));
					ps.setString(4, String.valueOf(hashDepthNum.get(entr)));
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			/////////////////////////////////外链带来的活跃访客数量。//////////
			for (String entry : hashOutChainNum.keySet()) {
				System.out.println(entry+"\t"+hashOutChainNum.get(entry));
				
			}
			//////////////////////////////////跳出个数及跳出率//////////////////////
			
			for(String enString:hashOutChainRateNum.keySet()){
				System.out.println(enString+"\t"+hashOutChainRateNum.get(enString));
			}
			
			
			/////////////////////////订单支付成功个数///////////////////
			ps=getPSSQL("ordersuccess_num");
			for (String entry :hashOrderSuccessNum.keySet()) {
				//System.out.println(entry+"\t"+hashOrderSuccessNum.get(entry));
				try {
					ps.setString(1, entry);
					ps.setString(2, String.valueOf(hashOrderSuccessNum.get(entry)) );
					
					ps.setString(3, String.valueOf(hashOrderSuccessNum.get(entry)) );
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			////////////////////////订单退款个数//////////////////////
			ps=getPSSQL("order_retreat");
			for (String entry : hashOrderRetreatNum.keySet()) {
				try {
					ps.setString(1, entry);
					ps.setString(2, String.valueOf(hashOrderRetreatNum.get(entry)) );
					
					ps.setString(3, String.valueOf(hashOrderRetreatNum.get(entry)) );
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			////////////////////////计算订单个数，计算订单金额和///////////////////////////////
			ps=getPSSQL("order_number_money");
			for (String entry : hashOrderMoney.keySet()) {
				//System.out.println(entry+"\t"+hashOrderMoney.get(entry));
				//System.out.println(entry+"\t pp"+hashOrderNumber.get(entry.split("\t")[0]));
				
				//订单个数去掉重复
				try {
					String[] vall=entry.split("\t");
					ps.setString(1,vall[0]);//日期
					ps.setString(2,vall[1]);//oid
					ps.setString(3,String.valueOf(hashOrderNumber.get(vall[0])) );
					ps.setString(4, String.valueOf(hashOrderMoney.get(entry)));
					
					ps.setString(5,String.valueOf(hashOrderNumber.get(vall[0])) );
					ps.setString(6, String.valueOf(hashOrderMoney.get(entry)));
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			
			// 关闭资源
			try {
				for (String entry : psMaps.keySet()) {
					psMaps.get(entry).close();
				}
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public PreparedStatement getPSSQL(String sqlName) {
			PreparedStatement ps = psMaps.get(sqlName);
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get(sqlName));
					psMaps.put(sqlName, ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return ps;
		}

	}

}
