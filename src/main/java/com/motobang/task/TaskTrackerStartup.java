package com.motobang.task;

import java.io.IOException;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.TaskTracker;
import com.motoband.manager.ConfigManager;
import com.motoband.manager.DBConnectionManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.utils.OkHttpClientUtil;

/**
 * 
 * Created by junfei.Yang on 2020年3月11日.
 */
public class TaskTrackerStartup {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TaskTrackerStartup.class);

    public static void main(String[] args) throws IOException {
    	DBConnectionManager.init();
		ConfigManager.getInstance().init("MotoBandService");
		OkHttpClientUtil.init();
        String cfgPath = args[0];
        start(cfgPath);
    }

    public static void start(String cfgPath) {
        try {
            TaskTrackerCfg cfg = TaskTrackerCfgLoader.load(cfgPath);

            final TaskTracker taskTracker;

            taskTracker = DefaultStartup.start(cfg);

            taskTracker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                	LOGGER.error("清理tasktracker");
                    taskTracker.stop();
                }
            }));

        } catch (CfgException e) {
            System.err.println("TaskTracker Startup Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
