package com.motobang.task;

public enum TaskEnum {

	PUSH("PUSH","推送业务"),
	RANK("RANK","排行榜，包含日月周年。"),
	ACTIVE("ACTIVE","日活");
	
	public String name;
	public String desc;
	TaskEnum(String name,String desc) {
		this.name=name;
		this.desc=desc;
	}
	
	
}
