package com.bf.log.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bf.log.constants.LogConstants;
import com.bf.log.dimention.DregionDimention;

public class DregionReducer extends Reducer<DregionDimention, LongWritable, DregionDimention, LongWritable> {
	@Override
	protected void reduce(DregionDimention arg0, Iterable<LongWritable> arg1,
			Reducer<DregionDimention, LongWritable, DregionDimention, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		long count=0;
		if(arg0.getLogType().equals("Dregion")){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			
		}
		
		if(arg0.getLogType().equals(LogConstants.OutNum)){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			if(count!=1){
				count=0;
			}
			else{
				count=1;
			}
			
		}
		
		if(arg0.getLogType().equals(LogConstants.OutChain)){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			System.out.println(arg0.toString());
		}
		
		if(arg0.getLogType().equals(LogConstants.OutChainNum)){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			if(count!=1){
				count=0;
			}
			//System.out.println(arg0.toString());
		}
		
		if(arg0.getLogType().equals(LogConstants.OrderSuccess)){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			
		}
		
		if(arg0.getLogType().equals(LogConstants.OrderRetreat)){
			for (LongWritable longWritable : arg1) {
				count++;
			}
			
		}
		
		if(arg0.getLogType().equals(LogConstants.OrderMoney)){
			for (LongWritable longWritable : arg1) {
				count+=longWritable.get();
			}
		}
		
		arg2.write(arg0, new LongWritable(count));
		
		
	}
}
