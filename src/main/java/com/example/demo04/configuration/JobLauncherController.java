package com.example.demo04.configuration;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.support.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RestController
public class JobLauncherController {

    @Autowired
    JobLauncher jobLauncher;
    @Autowired @Qualifier("JOB3")
    Job job;
    @Autowired
    private JobRegistry jobRegistry;
    @Autowired
    private StepRegistry stepRegistry;
    @Autowired
    private JobOperator jobOperator;
    @Autowired
    private JobExplorer jobExplorer;


    @RequestMapping("/reload")
    public String reloadJob() throws Exception  {
        //JobRegistry jobRegistry = new MapJobRegistry();
        //StepRegistry stepRegistry = new MapStepRegistry();
        DefaultJobLoader jobLoader = new DefaultJobLoader(jobRegistry, stepRegistry);
        GenericApplicationContextFactory factory = new GenericApplicationContextFactory(StepLoopConfiguration.class);
        //jobLoader.load(factory);
        //jobLoader.clear();
        jobLoader.reload(factory);
        return "RELOAD SUCCESS";
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String launch(@RequestParam("name") String jobName) throws Exception {
        try {
            //실행중인 job은 해당 이름에 end_time null 인경우 이다.
         Set<JobExecution> executions = jobExplorer.findRunningJobExecutions(jobName);
          if(executions.size() > 0 ) return "Already Job is running";
//         for(JobExecution execution : executions) {
//             if(execution.getStatus().equals("COMPLETED")) return "Already Job is running";
//         }
        //Configuration에 JobResisty 를 등록 한후 사용
        Job job = jobRegistry.getJob(jobName);
        //JobParameters jobParameters = new JobParameters();
        JobParameters param = new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
        //JobParameters param = new JobParametersBuilder().addDate("date", DateUtils.truncate(new Date(), Calendar.DATE)).toJobParameters();
        jobLauncher.run(job, param);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong");
            return String.valueOf(e);
        }
        return "Run!!!";
    }

    //현재 running 중인 job을 종료
    @RequestMapping(value = "/{jobName}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    public void stopJob(@PathVariable("jobName") String jobName) throws Exception {
        Set<JobExecution> executions = jobExplorer.findRunningJobExecutions(jobName);
        for(JobExecution execution : executions ){
            if (execution.getStatus() == BatchStatus.STARTED) {
                jobOperator.stop(execution.getId());
            }
        }
    }


//    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
//    @ResponseStatus(HttpStatus.OK)
//    public void stop(@PathVariable("id") Long id) throws Exception {
//        this.jobOperator.stop(id);
//    }

    //JobLauncher 를 이용한 실행
    @RequestMapping("/test")
    public String handle() throws Exception {
//        for(int i=0; i< 100; i++) {
//            JobParameters param = new JobParametersBuilder().addLong("timestamp", new Date().getTime()).toJobParameters();
        JobParameters param = new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
        //JobParameters param = new JobParametersBuilder().addDate("date", DateUtils.truncate(new Date(), Calendar.DATE)).toJobParameters();
        jobLauncher.run(job, param);
//            jobLauncher.run(job, new JobParameters());
        //       }
        return "Done!!!-!!!!!";
    }
}