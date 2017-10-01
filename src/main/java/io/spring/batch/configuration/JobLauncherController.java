package io.spring.batch.configuration;

import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.Set;

@RestController
public class JobLauncherController {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    @Qualifier("JOB3")
    Job job;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobExplorer jobExplorer;

    @RequestMapping("/test")
    public String handle() throws Exception {
        for(int i=0; i< 100; i++) {
            JobParameters param = new JobParametersBuilder().addLong("timestamp", new Date().getTime()).toJobParameters();
            jobLauncher.run(job, param);
//            jobLauncher.run(job, new JobParameters());

        }
        return "Done!!!-!!!!!";
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public long launch(@RequestParam("name") String name) throws Exception {
        return this.jobOperator.start("job", String.format("name=%s", name));
    }

//    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
//    @ResponseStatus(HttpStatus.OK)
//    public void stop(@PathVariable("id") Long id) throws Exception {
//        this.jobOperator.stop(id);
//    }

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
}