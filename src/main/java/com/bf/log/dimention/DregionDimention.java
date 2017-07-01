package com.bf.log.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DregionDimention implements WritableComparable<DregionDimention>{
	
	private String logDate="-";
	private String logIp="-";
	private String logUSD="-";//会话id
	
	private String logType="-";//标识做地域/用户访问深度分析分析
	
	private String logPURL="-";
	private String logUUD="-";
	private String logPREF="-";
	
	private String logEN="-";
	private String logOID="-";

	public String getLogDate() {
		return logDate;
	}

	public void setLogDate(String logDate) {
		this.logDate = logDate;
	}

	public String getLogIp() {
		return logIp;
	}

	public void setLogIp(String logIp) {
		this.logIp = logIp;
	}

	public String getLogUSD() {
		return logUSD;
	}

	public void setLogUSD(String logUSD) {
		this.logUSD = logUSD;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public String getLogPURL() {
		return logPURL;
	}

	public void setLogPURL(String logPURL) {
		this.logPURL = logPURL;
	}

	public String getLogUUD() {
		return logUUD;
	}

	public void setLogUUD(String logUUD) {
		this.logUUD = logUUD;
	}

	public String getLogPREF() {
		return logPREF;
	}

	public void setLogPREF(String logPREF) {
		this.logPREF = logPREF;
	}

	public String getLogEN() {
		return logEN;
	}

	public void setLogEN(String logEN) {
		this.logEN = logEN;
	}

	public String getLogOID() {
		return logOID;
	}

	public void setLogOID(String logOID) {
		this.logOID = logOID;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(logUSD);
		out.writeUTF(logDate);
		out.writeUTF(logEN);
		out.writeUTF(logIp);
		out.writeUTF(logPREF);
		out.writeUTF(logPURL);
		out.writeUTF(logType);
		out.writeUTF(logUUD);
		out.writeUTF(logOID);
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.logUSD=in.readUTF();
		this.logDate=in.readUTF();
		this.logEN=in.readUTF();
		this.logIp=in.readUTF();
		this.logPREF=in.readUTF();
		this.logPURL=in.readUTF();
		this.logType=in.readUTF();
		this.logUUD=in.readUTF();
		this.logOID=in.readUTF();
	}

	public int compareTo(DregionDimention arg0) {
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.logUSD.compareTo(arg0.logUSD);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logDate.compareTo(arg0.logDate);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logEN.compareTo(arg0.logEN);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logIp.compareTo(arg0.logIp);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logPREF.compareTo(arg0.logPREF);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logPURL.compareTo(arg0.logPURL);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logType.compareTo(arg0.logType);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logUUD.compareTo(arg0.logUUD);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logOID.compareTo(arg0.logOID);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}

	@Override
	public String toString() {
		return "DregionDimention [logDate=" + logDate + ", logIp=" + logIp + ", logUSD=" + logUSD + ", logType="
				+ logType + ", logPURL=" + logPURL + ", logUUD=" + logUUD + ", logPREF=" + logPREF + ", logEN=" + logEN
				+ ", logOID=" + logOID + "]";
	}

}
