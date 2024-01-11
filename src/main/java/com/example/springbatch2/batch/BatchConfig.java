package com.example.springbatch2.batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@EnableConfigurationProperties(BatchProperties.class)
public class BatchConfig extends DefaultBatchConfiguration {
    final TransactionTemplate transactionTemplate;

    @Override
    protected String getTablePrefix() {
        return "HENOK_";
    }

    final BatchProperties batchProperties;

    public BatchConfig(TransactionTemplate transactionTemplate, BatchProperties batchProperties) {
        this.transactionTemplate = transactionTemplate;
        this.batchProperties = batchProperties;
    }

    @Bean
   public JobLauncherApplicationRunner jobLauncherApplicationRunner(){
       return  new JobLauncherApplicationRunner(jobLauncher(),jobExplorer(),jobRepository());
   }

   @Bean
   @Primary
   public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer(){

       return  new BatchDataSourceScriptDatabaseInitializer(getDataSource(),batchProperties.getJdbc());
   }

    @Bean
    public Job job() {


        return new JobBuilder("job", jobRepository())
                .start(taskletStep()).next(builderStep()).build();
    }

    public Step taskletStep(){
        var taskletStep = new TaskletStep();
        taskletStep.setTasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                System.out.println("****** extecutee tasklet step ******");
                return RepeatStatus.FINISHED;
            }
        });
        taskletStep.setName("taskletStep");
        taskletStep.setJobRepository(jobRepository());
        taskletStep.setTransactionManager(getTransactionManager());
        return taskletStep;
    }
  public Step builderStep(){

        return  new StepBuilder("builderStep",jobRepository())
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                        System.out.println("****** builder Step ****** ");
                        return RepeatStatus.FINISHED;
                    }
                },getTransactionManager()).build();
  }
}
