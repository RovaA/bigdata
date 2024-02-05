package mu.rova.bigdata.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import mu.rova.bigdata.domain.Weather;
import mu.rova.bigdata.repository.WeatherRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class WeatherServiceImpl implements WeatherService {
	
	@Autowired
	private WeatherRepository weatherRepository;

	@Override
	public Flux<Weather> findAll() {
		return weatherRepository.findAll();
	}

	@Override
	public Mono<Weather> create(Weather weather) {
		return weatherRepository.save(weather);
	}

}
