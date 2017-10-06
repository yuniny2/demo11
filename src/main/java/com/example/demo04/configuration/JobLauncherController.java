package com.example.demo04.configuration;

import javafx.application.Application;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.support.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

@RestController
public class JobLauncherController {

    @Autowired// @Qualifier("JOB_LAUNCHER")
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired// @Qualifier("STEP_REGISTORY")
    private StepRegistry stepRegistry;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    public JobRepository jobRepository;

    /**
     * JobReload : 초기 STEP 의 DB 값으로 만든 dynamic STEP을 만들기 때문에 DB 값이 변경되면 STEP 도 변경시켜준다.
     * @return
     * @throws Exception
     */
    @RequestMapping("/reload")
    public String reloadJob() throws Exception  {
        //[001]기존의 JobRegistry, StepRegistry 를 Lodd 하여 관련 Configuration 을 찾아 Reload
        DefaultJobLoader jobLoader = new DefaultJobLoader(jobRegistry, stepRegistry);
        //GenericApplicationContextFactory factory = new GenericApplicationContextFactory(StepLoopConfiguration.class);
        ApplicationContextFactory factory = new GenericApplicationContextFactory(StepLoopConfiguration.class);
        jobLoader.reload(factory);
        return "RELOAD SUCCESS!!!";
    }

    /**
     * Job 을 실행시킨다. 반드시 Job job = jobRegistry.getJob(jobName); 가져와야 reload 된 job 이 실행된다.
     * @param jobName
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String launch(@RequestParam("name") String jobName) throws Exception {
        JobExecution jobExecution = null;
        try {
            //[FACT]실행중인 job은 해당 이름에 end_time null 인경우 이다.
            Set<JobExecution> executions = jobExplorer.findRunningJobExecutions(jobName);
            if(executions.size() > 0 ) return "Already Job is running";
            //[002]JobRegistory에 등록된 JOB 을 가져온다. : JobRegistry 에서 가져오지 않을 경우 reload 된 JOB이 호출되지 않음
            Job job = jobRegistry.getJob(jobName);
            JobParameters param = new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
            //JobParameters param = new JobParametersBuilder().addDate("date", DateUtils.truncate(new Date(), Calendar.DATE)).toJobParameters();
            jobExecution =jobLauncher.run(job, param);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Something went wrong");
            return String.valueOf(e);
        }
        return String.valueOf(jobExecution);
    }

    /**
     * 현재 running 중이 job 종료
     * @param jobName
     * @throws Exception
     */
    @RequestMapping(value = "/{jobName}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    public String stopJob(@PathVariable("jobName") String jobName) throws Exception {
        String result = "Running JOB IS Nothing!";
        //CASE1
//        Set<Long> executions = jobOperator.getRunningExecutions(jobName);
//        jobOperator.stop(executions.iterator().next());
        //CASE2
        Set<JobExecution> executions = jobExplorer.findRunningJobExecutions(jobName);
        for(JobExecution execution : executions ){
            if (execution.getStatus() == BatchStatus.STARTED) {
                jobOperator.stop(execution.getId());
                return jobName+" JOB IS STOPPED!";
            } else {
                return result;
            }
        }
        return result;
    }

//    @RequestMapping("/test")
//    public String handle() throws Exception {
//        ApplicationContext context = new AnnotationConfigApplicationContext(StepLoopConfiguration.class);
//        //method 이름이 Bean 이름
//        Job job1 = context.getBean("executeMyJob", Job.class);
//        Job job2 = context.getBean("executeMyJob", Job.class);
//        job1.hashCode();
//        job1.toString();
//        System.out.println("job1--->"+job1);
//        System.out.println("job2--->"+job2);
//        boolean test = job1 == job2;
//        return job1.toString();
//    }

    //JobParameters jobParameters = new JobParameters();
    //JobParameters param = new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
    //JobParameters param = new JobParametersBuilder().addDate("date", DateUtils.truncate(new Date(), Calendar.DATE)).toJobParameters();
}