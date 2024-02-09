package mu.rova.bigdata.config;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import mu.rova.bigdata.domain.Weather;

@Configuration
public class ChunkBatchConfiguration {
	
	@Bean(name = "importWeatherChunkJob")
	public Job importWeatherJob(@Qualifier("jobChunkRepository") JobRepository jobRepository, 
			@Qualifier("step1ChunkReader") Step step1) {
		return new JobBuilder("importWeatherChunkJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.start(step1)
				.build();
	}
	
	@Bean(name = "jobChunkRepository")
    public JobRepository getJobRepository(PlatformTransactionManager transactionManager, DataSource dataSource) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setTransactionManager(transactionManager);
        factory.setDataSource(dataSource);
        factory.afterPropertiesSet();
        return factory.getObject();
    }

	@Bean(name = "step1ChunkReader")
	public Step step1ChunkReader(@Qualifier("jobChunkRepository") JobRepository jobRepository, 
			PlatformTransactionManager transactionManager,
			WeatherChunkItemReader reader,
			WeatherChunkConsoleItemWriter writer,
			TaskExecutor taskExecutor) {
		return new StepBuilder("step1Chunk", jobRepository)
				.<List<Weather>, List<Weather>>chunk(1, transactionManager)
				.reader(reader)
				.writer(writer)
				.taskExecutor(taskExecutor)
				.startLimit(2)
				.faultTolerant()
				.retryLimit(2)
				.retry(Exception.class)
				.build();
	}
	
	@Bean
	@JobScope
	public WeatherChunkItemReader weatherChunkItemReader(@Value("#{jobParameters['BATCH_SIZE']}") Long batchSize) {
		return new WeatherChunkItemReader("weather_stations.csv", batchSize);
	}
	
	@Bean
	public WeatherChunkConsoleItemWriter weatherChunkConsoleItemWriter() {
		return new WeatherChunkConsoleItemWriter();
	}

}
