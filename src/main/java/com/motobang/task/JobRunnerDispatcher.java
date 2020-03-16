package com.motobang.task;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.json.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.MBResponse;
import com.motoband.common.MBResponseCode;
import com.motoband.model.task.MessageTaskModel;
import com.motobang.task.impl.IM_PUSH;
import com.motobang.task.impl.IM_PUSH_ERROR_USERIDS;
import com.motobang.task.impl.CREATE_MBUSER_PUSH;

public class JobRunnerDispatcher implements JobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(JobRunnerDispatcher.class);

    private static final ConcurrentHashMap<String/*type*/, JobRunner>
            JOB_RUNNER_MAP = new ConcurrentHashMap<String, JobRunner>();

    static {
        JOB_RUNNER_MAP.put(MessageTaskModel.IM_PUSH, new IM_PUSH()); 
        JOB_RUNNER_MAP.put(MessageTaskModel.CREATE_MBUSER_PUSH, new CREATE_MBUSER_PUSH());
        JOB_RUNNER_MAP.put(MessageTaskModel.IM_PUSH_ERROR_USERIDS, new IM_PUSH_ERROR_USERIDS());

    }


	@Override
	public Result run(JobContext arg0) throws Throwable {
        String type = arg0.getJob().getParam("type");
        if(StringUtils.isBlank(type)) {
        	LOGGER.error("taskid:"+arg0.getJob().getTaskId()+",执行失败,type参数为空");
        	return new Result(Action.EXECUTE_FAILED, JSON.toJSONString(MBResponse.getMBResponse(MBResponseCode.LTS_PARAM_NULL_ERROR)));
        }
        if(JOB_RUNNER_MAP.get(type)==null) {
        	LOGGER.error("taskid:"+arg0.getJob().getTaskId()+",执行失败,稍后继续尝试，策略为1分钟 2分钟 3分钟。最大为任务重试次数。,type参数的实现类没有找到!");
        	return new Result(Action.EXECUTE_LATER, JSON.toJSONString(MBResponse.getMBResponse(MBResponseCode.LTS_TASK_TYPE_NOT_FIND_ERROR)));
        }
        return JOB_RUNNER_MAP.get(type).run(arg0);
	}
}