package mu.rova.bigdata.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table(name = "weather")
public class Weather {
	
	@Id
	@Column(value = "id")
	private Long id;

	@Column(value = "country")
	private String country;

	@Column(value = "rank")
	private String rank;
	
	public Weather() {
	}

	public Weather(String country, String rank) {
		this.country = country;
		this.rank = rank;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}
	
}
