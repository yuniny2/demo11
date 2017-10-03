package com.example.demo04.configuration;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableBatchProcessing
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

    private ApplicationContext applicationContext;

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

    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
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
//    @Bean
//    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
//        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
//        registrar.setJobRegistry(jobRegistry);
//        return registrar;
//    }


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
        List<Step> steps = new ArrayList<Step>();
/*        String[] strs = {"1", "2", "3",};
        List<String> dates = Arrays.asList(strs);
        for (String date : dates) {
            steps.add(createStep(date));
        }
        */
//step loop
        List<Integer> stepTest = new ArrayList<>();
        for(int i=0; i<= 1000; i++ ){
            stepTest.add(i);
        }
        for (Integer date : stepTest) {
            steps.add(createStep(date));
        }

        return jobBuilderFactory.get("executeMyJob121")
                .start(createParallelFlow(steps))
                .end()
                .build();
    }

    private Step createStep(Integer date){
        return stepBuilderFactory.get("test_stop_001 : " + date)
                .tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello World!");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    private Flow createParallelFlow(List<Step> steps) {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        // max multithreading = -1, no multithreading = 1, smart size = steps.size()
        taskExecutor.setConcurrencyLimit(1);

        List<Flow> flows = steps.stream()
                .map(step -> new FlowBuilder<Flow>("flow_" + step.getName()).start(step).build())
                .collect(Collectors.toList());

        return new FlowBuilder<SimpleFlow>("parallelStepsFlow")
                .split(taskExecutor)
                .add(flows.toArray(new Flow[flows.size()]))
                .build();
    }
}
