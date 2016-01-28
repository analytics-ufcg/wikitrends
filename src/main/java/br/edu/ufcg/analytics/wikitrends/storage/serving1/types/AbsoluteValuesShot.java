package br.edu.ufcg.analytics.wikitrends.storage.serving1.types;

import java.io.Serializable;
import java.util.Set;

/**
 * 
 * Class that represents a stored value into the serving layer.
 * 
 * @author Guilherme Gadelha
 *
 */
public class AbsoluteValuesShot implements Serializable {
		private static final long serialVersionUID = 2644747318393169105L;
		
//		private UUID id;
		private Integer hour;
    	private Integer day;
    	private Integer month;
    	private Integer year;
        
    	private Long all_edits;
		private Long minor_edits;
    	private Long average_size;

        private Set<String> distinct_pages_set;
		private Set<String> distinct_editors_set;
		private Set<String> distinct_servers_set;

		private Long smaller_origin;

		public AbsoluteValuesShot(Long all_edits, Long minor_edits, Long average_size,
        							Set<String> distinct_pages_set, 
        							Set<String> distinct_editors_set, 
        							Set<String> distinct_servers_set, 
        							Long smaller_origin,
        							Integer year, 
        							Integer month, 
        							Integer day, 
        							Integer hour) {
            
//        	this.id = UUID.randomUUID();
        	this.all_edits = all_edits;
        	this.minor_edits = minor_edits;
        	this.average_size = average_size;
            this.distinct_pages_set = distinct_pages_set;
            this.distinct_editors_set = distinct_editors_set;
            this.distinct_servers_set = distinct_servers_set;
            this.smaller_origin = smaller_origin;
            
    		setYear(year);
    		setMonth(month);
    		setDay(day);
    		setHour(hour);
        }
        
		public AbsoluteValuesShot() {
			// TODO Auto-generated constructor stub
		}

//		public void setid(UUID id) {
//        	this.id = id;
//        }
//        
//        public UUID getid() {
//        	return id;
//        }

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

		public Long getAll_edits() {
			return all_edits;
		}

		public void setAll_edits(Long all_edits) {
			this.all_edits = all_edits;
		}

		public Long getMinor_edits() {
			return minor_edits;
		}

		public void setMinor_edits(Long minor_edits) {
			this.minor_edits = minor_edits;
		}

		public Long getAverage_size() {
			return average_size;
		}

		public void setAverage_size(Long average_size) {
			this.average_size = average_size;
		}

		public Long getSmaller_origin() {
			return smaller_origin;
		}

		public void setSmaller_origin(Long smaller_origin) {
			this.smaller_origin = smaller_origin;
		}

		@Override
		public String toString() {
			return "AbsoluteValuesShot [hour=" + hour + ", day=" + day + ", month=" + month + ", year=" + year
					+ ", all_edits=" + all_edits + ", minor_edits=" + minor_edits + ", average_size=" + average_size
					+ ", distinct_pages_set=" + distinct_pages_set + ", distinct_editors_set=" + distinct_editors_set
					+ ", distinct_servers_set=" + distinct_servers_set + ", smaller_origin=" + smaller_origin + "]";
		}
    }