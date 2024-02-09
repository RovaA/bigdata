package mu.rova.bigdata.config;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import mu.rova.bigdata.domain.Weather;

public class WeatherConsoleItemWriter implements ItemWriter<Weather> {

	@Override
	public void write(Chunk<? extends Weather> chunk) throws Exception {
		chunk.forEach(weather -> {
			System.out.println(weather.getCountry() + " " + weather.getRank());
		});
	}

}
