package mu.rova.bigdata.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;

import mu.rova.bigdata.domain.Weather;

@EnableBatchProcessing(transactionManagerRef = "transactionManager")
@Configuration
public class BatchConfiguration {
	
	/**
	 * Make spring batch parallel
	 * @return
	 */
	@Bean
	public TaskExecutor taskExecutor() {
	    return new SimpleAsyncTaskExecutor("spring_batch");
	}
	
	@Bean(name = "importWeatherJob")
	public Job importWeatherJob(JobRepository jobRepository, 
			Step step1) {
		return new JobBuilder("importWeatherJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.start(step1)
				.build();
	}
	
	@Bean(name = "jobRepository")
    public JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setTransactionManager(transactionManager());
        factory.setDataSource(dataSource());
        factory.afterPropertiesSet();
        return factory.getObject();
    }
	
	@Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }
    
	@Bean
    public DataSource dataSource() {
     EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
     return builder.setType(EmbeddedDatabaseType.H2)
           .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
           .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
           .build();
    }

	@Bean
	public Step step1(JobRepository jobRepository, 
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
