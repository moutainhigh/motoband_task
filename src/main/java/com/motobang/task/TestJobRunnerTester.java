package com.motobang.task;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobExtInfo;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.github.ltsopensource.tasktracker.runner.JobRunnerTester;

public class TestJobRunnerTester extends JobRunnerTester {

    public static void main(String[] args) throws Throwable {
        //  Mock Job 数据
        Job job = new Job();
        job.setTaskId("cms_"+"1111");
        job.setReplaceOnExist(true);
        job.setParam("type", "create_mbuser_push");
        job.setTaskTrackerNodeGroup("test_trade_TaskTracker");
        job.setTriggerTime(1583996730000l);
        job.setMaxRetryTimes(10);

        JobContext jobContext = new JobContext();
        jobContext.setJob(job);

        JobExtInfo jobExtInfo = new JobExtInfo();
        jobExtInfo.setRetry(false);

        jobContext.setJobExtInfo(jobExtInfo);

        // 运行测试
        TestJobRunnerTester tester = new TestJobRunnerTester();
        Result result = tester.run(jobContext);
        System.out.println(JSON.toJSONString(result));
    }

    @Override
    protected void initContext() {
        // TODO 初始化Spring容器
    }

    @Override
    protected JobRunner newJobRunner() {
        return new JobRunnerDispatcher();
    }
}