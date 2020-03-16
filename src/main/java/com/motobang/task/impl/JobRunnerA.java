package com.motobang.task.impl;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.logger.BizLogger;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.github.ltsopensource.tasktracker.runner.LtsLoggerFactory;
import com.motobang.task.TaskTrackerStartup;

/**
 * 推送任务
 * Created by junfei.Yang on 2020年3月12日.
 */
public class JobRunnerA implements JobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TaskTrackerStartup.class);


	@Override
	public Result run(JobContext jobContext) throws Throwable {
//		 BizLogger bizLogger = LtsLoggerFactory.getBizLogger();
		try {
            // TODO 业务逻辑
            // 会发送到 LTS (JobTracker上)
//            bizLogger.info("测试，业务日志啊啊啊啊啊");
//            bizLogger.info("jobContext="+JSON.toJSONString(jobContext));
			LOGGER.info("开始处理任务 jobContext="+JSON.toJSONString(jobContext));
			String taskid=jobContext.getJob().getTaskId();
			//标记需要处理的用户 条件是有效的用户  需要先筛选出有效的用户 条件是半年以内登录过的用户
			

        } catch (Exception e) {
        	LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
            return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        }
        return new Result(Action.EXECUTE_SUCCESS, "执行成功了，哈哈");
	}
}