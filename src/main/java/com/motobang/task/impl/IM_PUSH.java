package com.motobang.task.impl;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.MBMessageManager;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.model.BannerModel;
import com.motoband.model.MBMessageModel;
import com.motoband.model.SimpleUserModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.collection.CollectionUtil;
import com.motobang.task.TaskTrackerStartup;

/**
 * 推送任务
 * Created by junfei.Yang on 2020年3月12日.
 */
public class IM_PUSH implements JobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IM_PUSH.class);
	@Override
	public Result run(JobContext jobContext) throws Throwable {
//		 BizLOGGER bizLOGGER = LtsLOGGERFactory.getBizLOGGER();
		try {
            // TODO 业务逻辑
            // 会发送到 LTS (JobTracker上)
//            bizLOGGER.info("测试，业务日志啊啊啊啊啊");
//            bizLOGGER.info("jobContext="+JSON.toJSONString(jobContext));
			LOGGER.info("开始处理任务 jobContext="+JSON.toJSONString(jobContext));
			String taskid=jobContext.getJob().getTaskId();
			String data=jobContext.getJob().getParam("data");
			if(StringUtils.isNotBlank(data)) {
				MessageTaskModel taskModel=JSON.parseObject(data, MessageTaskModel.class);
				UserManager.getInstance().addMessageTask(taskModel);
				UserManager.getInstance().addMessageTaskUserAll(taskModel);
				LOGGER.error("taskid="+taskid+",开始查询推送的MBMessageModel");
				MBMessageModel model = gettaskMessageModel(taskModel);
				LOGGER.error("taskid="+taskid+",结束查询推送的MBMessageModel");
				String pushMsg = "您有一条新的消息，点击查看";
				if(model.bannermodel!=null){
					pushMsg =model.bannermodel.title;
				}
				//标记需要处理的用户 条件是有效的用户  需要先筛选出有效的用户 条件是半年以内登录过的用户
				FenPiSendtaskMsg_new(taskid, model, pushMsg, 0);

			}
			
			

        } catch (Exception e) {
        	LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
            return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        }
        return new Result(Action.EXECUTE_SUCCESS, "执行成功了，哈哈");
	}
	
	private void FenPiSendtaskMsg_new(String taskid, MBMessageModel model, String pushMsg, int pici) {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("taskid", taskid);
		dataMap.put("pici", pici * 80000);
		Clock c=Clock.systemUTC();
		long time=c.millis();
		LOGGER.error(
				String.format("taskid:%s,开始查询第 pici:%s 批次用户", taskid, pici));
		List<String> userids = UserManager.getInstance().getMessageTaskUserByTaskid(dataMap);
		if (LOGGER.isErrorEnabled()) {
			LOGGER.error(
					String.format("taskid:%s 结束查询第 pici:%s 批次用户,用时:%s,userids:%s", taskid, pici,c.millis()-time, userids==null?0:userids.size()));
		}
		if(userids==null||userids.size()==0) {
			return ;
		}
		LOGGER.error("taskid="+taskid+",开始推送");
		pici = batchSendCMSMessage(taskid, model, pushMsg, pici, userids);
		LOGGER.error("taskid="+taskid+",结束推送");
		FenPiSendtaskMsg_new(taskid, model, pushMsg, pici);		
	}

	private int batchSendCMSMessage(String taskid, MBMessageModel model, String pushMsg, int pici, List<String> userids) {
		if (userids != null && userids.size() > 0) {
			LOGGER.error("taskid="+taskid+",开始多线程执行推送任务,多线程数量"+Runtime.getRuntime().availableProcessors()*10);
			List<List<String>> res = CollectionUtil.averageAssign(userids, Runtime.getRuntime().availableProcessors()*10);
//			List<List<String>> res = CollectionUtil.averageAssign(userids, 50);
			CyclicBarrier cb = new CyclicBarrier(res.size() + 1);
			AtomicInteger groupcountAtomic = new AtomicInteger(0);
			for (List<String> innerlist : res) {
				final int thread_pici = pici;
				ExecutorsUtils.getInstance().submit(new Runnable() {
					@Override
					public void run() {
						if(CollectionUtil.isEmpty(innerlist)) {
							try {
								cb.await();
							} catch (InterruptedException | BrokenBarrierException e) {
								e.printStackTrace();
							}
						}
						int groupcount = groupcountAtomic.incrementAndGet();
						double forcountdouble = Math.ceil(innerlist.size() / 500.0);
						int forcount = (int) forcountdouble;
						LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行用户数量="+innerlist.size()+",分组数量="+forcount);
						List<List<String>> msglist = CollectionUtil.averageAssign(innerlist, forcount);
						int classcount = 1;
						for (List<String> sendlist : msglist) {
							if (LOGGER.isErrorEnabled()) {
//								LOGGER.error(String.format("taskid:%s,pici:%s,groupcount:%s,classcount:%s", taskid,
//										thread_pici, groupcount, classcount));
								LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行用户数量="+innerlist.size()+",分组数量="+forcount
										+",开始执行第"+classcount+"分组");

							}
							classcount++;
							singleSendtaskMsg(taskid, model, pushMsg, sendlist);
							if (LOGGER.isErrorEnabled()) {
//								LOGGER.error(String.format("taskid:%s,pici:%s,groupcount:%s,classcount:%s", taskid,
//										thread_pici, groupcount, classcount));
								LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行用户数量="+innerlist.size()+",分组数量="+forcount
										+",结束执行第"+classcount+"分组");
							}
							// System.out.print("====400===");
							// System.out.println("====400===");
						}

						try {
//							executor.submit(new Runnable() {
//							@Override
//							public void run() {
								
//							}
//						});
							cb.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							if (LOGGER.isErrorEnabled()) {
								LOGGER.error(ExceptionUtils.getStackTrace(e));
							}
						}

					}

				});
			}
			try {
				cb.await();
				pici++;
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(String.format("taskid:%s,pici:%s is over,start next", taskid, pici - 1));
				}

			} catch (InterruptedException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(ExceptionUtils.getStackTrace(e));
				}
			} catch (BrokenBarrierException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return pici;
	}

	protected void singleSendtaskMsg(String taskid, MBMessageModel model, String pushMsg, List<String> sendlist) {
		model.taskid=taskid;
		MBMessageManager.getInstance().sendMessage(model, sendlist, pushMsg);
	}

	public MBMessageModel gettaskMessageModel(MessageTaskModel messageTaskModel) {

		MBMessageModel model = new MBMessageModel();
		BannerModel bannermodel = new BannerModel();
		if (messageTaskModel != null) {
			bannermodel.title=messageTaskModel.title;
			bannermodel.subtitle=messageTaskModel.subtitle;
			bannermodel.des=messageTaskModel.des;
			bannermodel.linktype=messageTaskModel.linktype;
			bannermodel.linkurl=messageTaskModel.linkurl;
			bannermodel.imgurl=messageTaskModel.imgurl;
			bannermodel.gpid=messageTaskModel.gpid;
			bannermodel.nid=messageTaskModel.nid;
			if(StringUtils.isNotBlank(bannermodel.nid)) {
				String type=RedisManager.getInstance().hget(Consts.REDIS_SCHEME_NEWS, messageTaskModel.nid+"_ninfo", "type");
				if(StringUtils.isNotBlank(type)) {
					bannermodel.ntype=Integer.parseInt(type);
				}else {
					bannermodel.ntype=0;
				}
			}
			bannermodel.keyword=messageTaskModel.keyword;
			bannermodel.secondcarid=messageTaskModel.secondcarid;
			bannermodel.miniprogramid=messageTaskModel.miniprogramid;
			bannermodel.buserid=messageTaskModel.buserid;
			bannermodel.groupid=messageTaskModel.groupid;
		}
		model.bannermodel=bannermodel;
		model.msgtype=MBMessageModel.MBMsgType_Banner;
		model.msgtime=System.currentTimeMillis();
		model.content=messageTaskModel.title;
		SimpleUserModel simpleUserInfo = UserManager.getInstance().getSimpleUserInfo(Consts.TIM_ACTIVITYCENTERID);
		if (simpleUserInfo != null) {
			model.simpleusermodel=simpleUserInfo;
		}
		return model;

	}
}