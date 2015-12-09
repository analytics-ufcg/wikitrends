package br.edu.ufcg.analytics.wikitrends.storage.serving1.types;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 
 * Class that represents a stored value into the serving layer.
 * 
 * @author Guilherme Gadelha
 *
 */
public class AbsoluteValuesShot implements Serializable {
		private static final long serialVersionUID = 2644747318393169105L;
		
		private UUID id;
		private Integer hour;
    	private Integer day;
    	private Integer month;
    	private Integer year;
        
        private Map<String, Long> edits_data;

        private Set<String> distinct_pages_set;
		private Set<String> distinct_editors_set;
		private Set<String> distinct_servers_set;

		private Long smaller_origin;

		public AbsoluteValuesShot(Map<String, Long> edits_data, 
        							Set<String> distinct_pages_set, 
        							Set<String> distinct_editors_set, 
        							Set<String> distinct_servers_set, 
        							Long smaller_origin,
        							Integer year, 
        							Integer month, 
        							Integer day, 
        							Integer hour) {
            
        	this.id = UUID.randomUUID();
            this.edits_data = edits_data;
            this.distinct_pages_set = distinct_pages_set;
            this.distinct_editors_set = distinct_editors_set;
            this.distinct_servers_set = distinct_servers_set;
            this.smaller_origin = smaller_origin;
            
    		setYear(year);
    		setMonth(month);
    		setDay(day);
    		setHour(hour);
        }
        
		public void setid(UUID id) {
        	this.id = id;
        }
        
        public UUID getid() {
        	return id;
        }

		public Integer getDay() {
			return day;
		}

		public void setDay(Integer day) {
			this.day = day;
		}

		public Integer getMonth() {
			return month;
		}

		public void setMonth(Integer month) {
			this.month = month;
		}

		public Integer getYear() {
			return year;
		}

		public void setYear(Integer year) {
			this.year = year;
		}

		public Integer getHour() {
			return hour;
		}

		public void setHour(Integer hour) {
			this.hour = hour;
		}
		
		public Set<String> getDistinct_pages_set() {
			return distinct_pages_set;
		}

		public void setDistinct_pages_set(Set<String> distinct_pages_set) {
			this.distinct_pages_set = distinct_pages_set;
		}

		public Set<String> getDistinct_editors_set() {
			return distinct_editors_set;
		}

		public void setDistinct_editors_set(Set<String> distinct_editors_set) {
			this.distinct_editors_set = distinct_editors_set;
		}

		public Set<String> getDistinct_servers_set() {
			return distinct_servers_set;
		}

		public void setDistinct_servers_set(Set<String> distinct_servers_set) {
			this.distinct_servers_set = distinct_servers_set;
		}

		public Map<String, Long> getEdits_data() {
			return edits_data;
		}

		public void setEdits_data(Map<String, Long> edits_data) {
			this.edits_data = edits_data;
		}

		public Long getSmaller_origin() {
			return smaller_origin;
		}

		public void setSmaller_origin(Long smaller_origin) {
			this.smaller_origin = smaller_origin;
		}

		@Override
		public String toString() {
			return "AbsoluteValuesShot [id=" + id + ", hour=" + hour + ", day=" + day + ", month=" + month + ", year="
					+ year + ", edits_data=" + edits_data + ", distincts_pages_set=" + distinct_pages_set
					+ ", distincts_editors_set=" + distinct_editors_set + ", distincts_servers_set="
					+ distinct_servers_set + ", smaller_origin=" + smaller_origin + "]";
		}

    }