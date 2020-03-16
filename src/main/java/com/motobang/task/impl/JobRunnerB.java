package com.motobang.task.impl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.dao.UserDAO;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.search.ElasticSearchManager;
import com.motoband.model.CityDataModel;
import com.motoband.model.GarageModel;
import com.motoband.model.MBUserPushModel;
import com.motoband.model.NewsModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.collection.CollectionUtil;

/**
 * 筛选有效用户 
 * 创建一个新的用户表，同步到ES
 * 目前支持的字段
 * userid,province,city,gender, model,brand ,addtime,lastactivetime,ctype
 * Created by junfei.Yang on 2020年3月12日.
 */
public class JobRunnerB implements JobRunner {
	public static void main(String[] args) {
		for (int i = 0; i < 6; i++) {
			String d=LocalDate.now().plusMonths(-i).format(DateTimeFormatter.ofPattern("_yyyy_M"));
			System.out.println(d);
		}
		System.out.println(LocalDateTime.of(LocalDate.now().plusYears(-2), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
	}

	@Override
	public Result run(JobContext arg0) throws Throwable {
		long minaddtime=LocalDateTime.of(LocalDate.now().plusYears(-2), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		String sql="select userid,city,gender,addtime from mbuser where addtime>="+minaddtime+" and channel not like '%X' and userid not in (select userid from mbuser_push ) limit 1100";
		List<Map<String, Object>> result=UserDAO.executesql(sql);
		List<String> mbusermodeljsonstr=Lists.newArrayList();
		List<String> userids=Lists.newArrayList();
		for (Map<String, Object> map : result) {
			String userid=(String) map.get("userid");
			MBUserPushModel mbuser=new MBUserPushModel();
			if(map.containsKey("city")) {
				String city=(String) map.get("city");
				CityDataModel citydata=MotoDataManager.getInstance().getCityDataReverse(city);
				if(citydata!=null) {
					mbuser.province=citydata.province;
					mbuser.city=citydata.name;
				}else{
					 citydata=MotoDataManager.getInstance().getCityData(city);
							 if(citydata!=null) {
									mbuser.province=citydata.province;
									mbuser.city=citydata.name;
								}
				}
			}
			if(map.containsKey("gender")) {
				mbuser.gender=(int) map.get("gender");
			}
			String selectGrage="select * from usergarage where userid=\""+userid+"\" and `use`=1";
			List<Map<String, Object>> grageList=UserDAO.executesql(selectGrage);
			for (Map<String, Object> garageMap : grageList) {
				GarageModel g=JSON.parseObject(JSON.toJSONString(garageMap), GarageModel.class);
				mbuser.brandid=g.brandid;
				mbuser.modelid=g.modelid;
			}
			if(map.containsKey("addtime")) {
				mbuser.addtime=Long.parseLong(map.get("addtime").toString());
			}
			
			StringBuffer lastActiveTimeSQL=new StringBuffer();
//			String tableName=D
			//一年内没有登录过 标记为无效用户
			int year=12;
			lastActiveTimeSQL.append("select * from  (");

			for (int i = 0; i < year; i++) {
				lastActiveTimeSQL.append("select * from userloginonlog");
				lastActiveTimeSQL.append(LocalDate.now().plusMonths(-i).format(DateTimeFormatter.ofPattern("_yyyy_M")));
				lastActiveTimeSQL.append(" where userid=\""+userid+"\" and ctype in (1,2) and channel is null  limit 1");
				if (i != year-1) {
					lastActiveTimeSQL.append(" UNION ALL ");
				}
			}
			lastActiveTimeSQL.append(") as  t");

			List<Map<String, Object>> lastActiveTimeMapList=UserDAO.executesql(lastActiveTimeSQL.toString());
			if(!CollectionUtil.isEmpty(lastActiveTimeMapList)) {
				Map<String,Object> lastActiveTime=lastActiveTimeMapList.get(0);
				if(lastActiveTime.containsKey("ctype")) {
					mbuser.ctype=(int) lastActiveTime.get("ctype");
				}
				if(lastActiveTime.containsKey("cversion")) {
					 String cversion=lastActiveTime.get("cversion").toString().replaceAll("\\.","");
					 mbuser.cversion=Long.parseLong(cversion);
				}
			}
			UserDAO.inserUserPush(mbuser);
			mbusermodeljsonstr.add(JSON.toJSONString(mbuser));
			userids.add(userid);
			if(mbusermodeljsonstr.size()%1000==0) {
				Map<String,Object> searchparams=Maps.newHashMap();
				searchparams.put("searchcontent", mbusermodeljsonstr);
				searchparams.put("nids", userids);
				ElasticSearchManager.getInstance().syncAddEsList(MBUserPushModel.class, JSON.toJSONString(searchparams));	
				mbusermodeljsonstr.clear();
				userids.clear();
			}
		}
		Map<String,Object> searchparams=Maps.newHashMap();
		searchparams.put("searchcontent", mbusermodeljsonstr);
		searchparams.put("nids", userids);
		ElasticSearchManager.getInstance().syncAddEsList(MBUserPushModel.class, JSON.toJSONString(searchparams));	
		return new Result(Action.EXECUTE_SUCCESS);
	}
}