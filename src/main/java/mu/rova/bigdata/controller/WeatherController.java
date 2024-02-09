package mu.rova.bigdata.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import mu.rova.bigdata.domain.Weather;
import mu.rova.bigdata.service.WeatherService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/weathers")
public class WeatherController {
	
	@Autowired
	private WeatherService weatherService;

	@Autowired
	private JobLauncher jobLauncher;

	@Qualifier("importWeatherChunkJob")
	@Autowired
	private Job job;

	@GetMapping(produces = "application/stream+json")
	public Flux<Weather> findAll() {
		return weatherService.findAll();
	}	

	@PostMapping
	public Mono<Weather> create(@RequestBody Weather weather) {
		return weatherService.create(weather);
	}	

	@GetMapping("job")
	public void launchJob() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder().addLong("BATCH_SIZE", 20L).toJobParameters();
		jobLauncher.run(job, jobParameters);
	}	
	
}
