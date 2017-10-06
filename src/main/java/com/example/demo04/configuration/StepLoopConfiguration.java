package com.example.demo04.configuration;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapStepRegistry;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Configuration
@EnableBatchProcessing
@Import({DataSourceConfiguration.class, SampleIncrementer.class})
public class StepLoopConfiguration {
//    @Autowired
//    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    @Qualifier("mainDataSource")
    public DataSource dataSource;

    @Autowired
    public SampleIncrementer jobParametersIncrementer;

    List<Map<String, Object>> names;
    List<Step> steps = new ArrayList<>();
    private final ConcurrentMap<String, Map<String, Step>> map = new ConcurrentHashMap<String, Map<String, Step>>();
    final String jobName = "executeMyJob138";

    @PostConstruct
    public List<Map<String, Object>> init() {
       JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
       names = jdbcTemplate.queryForList("select name from name ");
       return names;
    }

    @Bean
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
        registrar.setJobRegistry(jobRegistry);
        return registrar;
    }

//    @Bean(name="JOB_LAUNCHER")
//    public JobLauncher jobLauncher(JobRepository jobRepository) {
//        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
//        //TaskExcutor 를 동기 또는 비동기로 변경 가능(설정을 안할시 동기라고 하나 Controller 에서 JobLauncher 를  Autowired 하면 기본 비동기)
//        //아래와 같이 동기로 설정하고 해당 Bean 을 Autowired 하면 동기로 설정됨.
//        TaskExecutor taskExecutor = new SyncTaskExecutor();
//        //TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
//        jobLauncher.setTaskExecutor(taskExecutor);
//        jobLauncher.setJobRepository(jobRepository);
//        return jobLauncher;
//    }
    @Bean
    public Job executeMyJob() {
        List<String> stepTest = new ArrayList<>();
        for(int i=0; i< names.size(); i++ ){
            stepTest.add(names.get(i).toString());
        }
        int i = 1;
        for (String name : stepTest) {
            steps.add(createStep(name+" :: "+i));
            i++;
       }
        //step 만 등록
        SimpleJob job = new SimpleJob(this.jobName);
        job.setJobRepository(this.jobRepository);
        job.setJobParametersIncrementer(this.jobParametersIncrementer);
        for(Step step : steps){
            job.addStep(step);
        }
        return job;
        //split flow 까지 등록
//        return jobBuilderFactory.get(this.jobName)
//                .start(createParallelFlow(steps))
//                .end()
//                .build();
    }

    private Step createStep(String name){
        return stepBuilderFactory.get("test_stop : ["+ name.toString()+"]")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("Hello World! ["+name+"]");
                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    @Bean(name="STEP_REGISTORY")
    public StepRegistry setStepRegistry() {
        StepRegistry stepRegistry = new MapStepRegistry();
        final Map<String, Step> jobSteps = new HashMap<>();
        for (Step step : steps) {
            jobSteps.put(step.getName(), step);
        }
        final Object previousValue = map.putIfAbsent(jobName, jobSteps);
//        if (previousValue != null) {
//            throw new DuplicateJobException("A job configuration with this name [ " + this.jobName + "] was already registered");
//        }
        return stepRegistry;
    }
    private Flow createParallelFlow(List<Step> steps) {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        // max multithreading = -1, no multithreading = 1, smart size = steps.size()
        simpleAsyncTaskExecutor.setConcurrencyLimit(1);

        List<Flow> flows = steps.stream()
                .map(step -> new FlowBuilder<Flow>("flow_" + step.getName()).start(step).build())
                .collect(Collectors.toList());

        return new FlowBuilder<SimpleFlow>("parallelStepsFlow")
                .split(simpleAsyncTaskExecutor)
                .add(flows.toArray(new Flow[flows.size()]))
                .build();
    }


}
