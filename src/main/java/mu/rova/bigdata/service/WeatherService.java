package mu.rova.bigdata.service;

import mu.rova.bigdata.domain.Weather;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface WeatherService {
	
	Flux<Weather> findAll();

	Mono<Weather> create(Weather weather);

}
