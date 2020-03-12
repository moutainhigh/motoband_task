package com.motobang.task.impl;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.dao.UserDAO;
import com.motoband.utils.ExecutorsUtils;

/**
 * 筛选有效用户 
 * 创建一个新的用户表，同步到ES
 * 目前支持的字段
 * userid,province,city,gender, model,brand ,addtime,lastactivetime
 * Created by junfei.Yang on 2020年3月12日.
 */
public class JobRunnerB implements JobRunner {

	@Override
	public Result run(JobContext arg0) throws Throwable {
		String sql="select userid from mbuser where channel not like '%X' and userid not in (select userid from mbuser_push ) limit 10";
		Map<String,Object> result=UserDAO.executesql(sql);
		System.out.println(JSON.toJSONString(result));
		return new Result(Action.EXECUTE_SUCCESS);
	}
}