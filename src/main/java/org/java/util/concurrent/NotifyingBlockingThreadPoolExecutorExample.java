package org.java.util.concurrent;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

//import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.Logger;

/**
 * Example of use of the NotifyingBlockingThreadPoolExecutor
 * @author Amir Kirsh & Yaneeve Shekel
 */
public class NotifyingBlockingThreadPoolExecutorExample {	

	//private static final Logger LOGGER = Logger.getLogger(NotifyingBlockingThreadPoolExecutorExample.class);

	//===================
	// M A I N
	//===================
	public static void main(String[] args) {

		//BasicConfigurator.configure();

		//-----------------------------------------------------------------
		// NotifyingBlockingThreadPoolExecutor initialization parameters
		//-----------------------------------------------------------------
		int poolSize = 5;
		int queueSize = 10; // recommended - twice the size of the poolSize
		int threadKeepAliveTime = 15;
		TimeUnit threadKeepAliveTimeUnit = TimeUnit.SECONDS;
		int maxBlockingTime = 10;
		TimeUnit maxBlockingTimeUnit = TimeUnit.MILLISECONDS;
		Callable<Boolean> blockingTimeoutCallback = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				//LOGGER.info("*** Still waiting for task insertion... ***");
				return true; // keep waiting
			}
		};
		
		//-----------------------------------------------------------------
		// Create the NotifyingBlockingThreadPoolExecutor
		//-----------------------------------------------------------------
		NotifyingBlockingThreadPoolExecutor threadPoolExecutor =
			new NotifyingBlockingThreadPoolExecutor(poolSize, queueSize,
											threadKeepAliveTime, threadKeepAliveTimeUnit,
											maxBlockingTime, maxBlockingTimeUnit, blockingTimeoutCallback);

		//-----------------------------------------------------------------
		// Create the tasks for the NotifyingBlockingThreadPoolExecutor
		//-----------------------------------------------------------------
		for (int i = 0; i < 100; i++) {
			try {
				threadPoolExecutor.execute(new MyNaiveTask("Task_" + (i+1)));
			} catch (Throwable e){
				//LOGGER.error("An exception when sending task: " + (i+1), e);
				continue; // waive rejected or failed tasks
			}
			//LOGGER.info("A task was sent successfully: " + (i+1));
		}

		//-----------------------------------------------------------------
		// All tasks were sent...
		//-----------------------------------------------------------------
		//LOGGER.info("Almost Done...");

		//-----------------------------------------------------------------
		// Wait for the last tasks to be done
		//-----------------------------------------------------------------
		try {
			boolean done = false;
			do {
				//LOGGER.info("waiting for the last tasks to finish");
				// we don't want to use here awaitTermination, which is relevant only if shutdown was called
				// in our case we don't call shutdown - we want to know that all tasks sent so far are finished
				done = threadPoolExecutor.await(20, TimeUnit.MILLISECONDS);
			} while(!done);
		} catch (InterruptedException e) {
			//LOGGER.error(e);
		}

		//LOGGER.info("DONE!");
	}
	// End of MAIN
	
	//================================================================
	// a static inner class for the example task we want to perform
	//================================================================
	static private class MyNaiveTask implements Runnable {  

		//private static final Logger LOGGER = Logger.getLogger(MyNaiveTask.class);

		private String name;

		public MyNaiveTask(String name) {
			this.name = name;
		}

		@Override
		public void run() {

			//LOGGER.info(name + " --- STARTED");
			Random random = new Random();
			int millis = random.nextInt(51);
			try {
				Thread.sleep(millis);
				//LOGGER.info(name + " --- FINISHED");
			}
			catch (InterruptedException e) {
				//LOGGER.warn(name + " sleep was interrupted...", e);
			}            

		}
		
		@Override
		public String toString() {
			return name;
		}
	}
	//================================================================

	
}
