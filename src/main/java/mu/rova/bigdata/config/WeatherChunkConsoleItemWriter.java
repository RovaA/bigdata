package mu.rova.bigdata.config;

import java.util.List;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import mu.rova.bigdata.domain.Weather;

public class WeatherChunkConsoleItemWriter implements ItemWriter<List<Weather>> {

	@Override
	public void write(Chunk<? extends List<Weather>> chunk) throws Exception {
		chunk.forEach(chunkItem -> {
			System.out.println("==============");
			chunkItem.forEach(weather -> {
				System.out.println(weather.getCountry() + " " + weather.getRank());
			});
			System.out.println("===============");
		});
	}

}
