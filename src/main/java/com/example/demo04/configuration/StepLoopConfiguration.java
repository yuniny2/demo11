package com.example.demo04.configuration;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.StepRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.DefaultJobLoader;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapStepRegistry;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.SimpleThreadPoolTaskExecutor;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Configuration
@EnableBatchProcessing
@Import(value=DataSourceConfiguration.class)
public class StepLoopConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    public JobExplorer jobExplorer;
    @Autowired
    public JobRepository jobRepository;
    @Autowired
    public JobRegistry jobRegistry;
    @Autowired
    public JobLauncher jobLauncher;
//    @Autowired
//    public TaskExecutor taskExecutor;
    @Autowired
    @Qualifier("mainDataSource")
    public DataSource dataSource;

    List<Map<String, Object>> names;
    List<Step> steps = new ArrayList<>();
    private final ConcurrentMap<String, Map<String, Step>> map = new ConcurrentHashMap<String, Map<String, Step>>();
    final String jobName = "executeMyJob137";

    @PostConstruct
    public List<Map<String, Object>> init() {
       JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
       names = jdbcTemplate.queryForList("select name from name ");
       return names;
    }

    @Bean
    public StepRegistry setStepRegistry() {
        StepRegistry stepRegistry = new MapStepRegistry();
        final Map<String, Step> jobSteps = new HashMap<>();
        for (Step step : steps) {
            jobSteps.put(step.getName(), step);
        }
        final Object previousValue = map.putIfAbsent(jobName, jobSteps);
//        if (previousValue != null) {
//            throw new DuplicateJobException("A job configuration with this name [ " + this.jobName
//                    + "] was already registered");
//        }
        return stepRegistry;
    }

    @Bean
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
        registrar.setJobRegistry(jobRegistry);
        return registrar;
    }
    @Bean
    public JobRepository jobRepository() {
        MapJobRepositoryFactoryBean factoryBean = new MapJobRepositoryFactoryBean(new ResourcelessTransactionManager());
        try {
            JobRepository jobRepository = factoryBean.getObject();
            return jobRepository;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    @Bean(name="JOBLAUNCHER")
    public JobLauncher jobLauncher(JobRepository jobRepository) {

        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        TaskExecutor taskExecutor = new SyncTaskExecutor();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        return jobLauncher;
    }
    @Bean
    public JobOperator jobOperator() throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();
        simpleJobOperator.setJobLauncher(this.jobLauncher);
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(this.jobRepository);
        simpleJobOperator.setJobExplorer(this.jobExplorer);
        simpleJobOperator.setJobRegistry(this.jobRegistry);
        simpleJobOperator.afterPropertiesSet();
        return simpleJobOperator;
    }

    @Bean(name="JOB3")
    public Job executeMyJob() {
        List<Integer> stepTest = new ArrayList<>();
//        for(int i=0; i<= 1000; i++ ){
//            stepTest.add(i);
//        }
        for(int i=0; i< names.size(); i++ ){
            stepTest.add(i);
        }
        for (Integer date : stepTest) {
            steps.add(createStep(date));
        }
        //step 만 등록
        SimpleJob job = new SimpleJob(this.jobName);
        job.setJobRepository(this.jobRepository);
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


    private Step createStep(Integer index){
        return stepBuilderFactory.get("test_stop : " + index)
                .tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello World! ["+index+"]");
                return RepeatStatus.FINISHED;
            }
        }).build();
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

    //    @Bean
//    public JobRegistryBeanPostProcessor jobRegistrar() throws Exception {
//        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
//
//        registrar.setJobRegistry(this.jobRegistry);
//        registrar.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
//        registrar.afterPropertiesSet();
//
//        return registrar;
//    }
}
