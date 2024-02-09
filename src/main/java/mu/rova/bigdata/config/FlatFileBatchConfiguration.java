package mu.rova.bigdata.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import mu.rova.bigdata.domain.Weather;

@Configuration
public class FlatFileBatchConfiguration {
	
	@Bean(name = "importWeatherFlatFileJob")
	public Job importWeatherJob(@Qualifier("jobFlatFileRepository") JobRepository jobRepository, 
			@Qualifier("step1WithFlatItemReader") Step step1) {
		return new JobBuilder("importWeatherFlatFileJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.start(step1)
				.build();
	}
	
	@Bean(name = "jobFlatFileRepository")
    public JobRepository getJobRepository(PlatformTransactionManager transactionManager, DataSource dataSource) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setTransactionManager(transactionManager);
        factory.setDataSource(dataSource);
        factory.afterPropertiesSet();
        return factory.getObject();
    }

	@Bean(name = "step1WithFlatItemReader")
	public Step step1WithFlatItemReader(@Qualifier("jobFlatFileRepository") JobRepository jobRepository, 
			FlatFileItemReader<Weather> reader,
			PlatformTransactionManager transactionManager,
			JdbcBatchItemWriter<Weather> writer,
			TaskExecutor taskExecutor) {
		return new StepBuilder("step1", jobRepository)
				.<Weather, Weather>chunk(100, transactionManager)
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
	public FlatFileItemReader<Weather> reader() {
		return new FlatFileItemReaderBuilder<Weather>()
				.name("weatherItemReader")
				.resource(new ClassPathResource("weather_stations.csv"))
				.delimited()
	            .delimiter(";")
	            .names("country", "rank")
	            .fieldSetMapper(new WeatherFieldSetMapper())
				.build();
	}
	
	@Bean
	public JdbcBatchItemWriter<Weather> jdbcBatchItemWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Weather> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
		jdbcBatchItemWriter.setAssertUpdates(true);
	    jdbcBatchItemWriter.setDataSource(dataSource);
	    jdbcBatchItemWriter.setSql("INSERT INTO weather (country, rank) VALUES (:country, :rank) ");
	    jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Weather>());
	    return jdbcBatchItemWriter;
	}
	
//	@Bean
//	public RepositoryItemWriter<Weather> repositoryItemWriter(WeatherRepository weatherRepository) {
//		RepositoryItemWriter<Weather> repositoryItemWriter = new RepositoryItemWriter<Weather>();
//		repositoryItemWriter.setRepository((CrudRepository<Weather, ?>) weatherRepository);
//        repositoryItemWriter.setMethodName("save");
//		return repositoryItemWriter;
//	}
	
//	@Bean
//	public WeatherItemWriter weatherItemWriter() {
//		return new WeatherItemWriter();
//	}

}
