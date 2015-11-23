package br.edu.ufcg.analytics.wikitrends.storage.serving.types;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.joda.time.DateTime;

/**
 * 
 * Class that represents a stored value into the serving layer.
 * 
 * @author Guilherme Gadelha
 *
 */
public class AbsoluteValueShot2 implements Serializable {
		private static final long serialVersionUID = 2644747318393169105L;
		
		private UUID id;
		private Integer hour;
    	private Integer day;
    	private Integer month;
    	private Integer year;
        private Date event_time;
        
        private Map<String, Long> edits_data;

        private Set<String> distincts_pages_set;
		private Set<String> distincts_editors_set;
		private Set<String> distincts_servers_set;

		private Long smaller_origin;

		public AbsoluteValueShot2(Map<String, Long> edits_data, 
        							Set<String> distincts_pages_set, 
        							Set<String> distincts_editors_set, 
        							Set<String> distincts_servers_set, 
        							Long smaller_origin) {
            
        	this.id = UUID.randomUUID();
            this.edits_data = edits_data;
            this.distincts_pages_set = distincts_pages_set;
            this.distincts_editors_set = distincts_editors_set;
            this.distincts_servers_set = distincts_servers_set;
            this.smaller_origin = smaller_origin;
            
            DateTime date = new DateTime();
            setEvent_time(date.toDate());
            
    		setYear(date.getYear());
    		setMonth(date.getMonthOfYear());
    		setDay(date.getDayOfMonth());
    		setHour(date.getHourOfDay());
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
		
		public Date getEvent_time() {
			return event_time;
		}

		public void setEvent_time(Date event_time) {
			this.event_time = event_time;
		}

		public Set<String> getDistincts_pages_set() {
			return distincts_pages_set;
		}

		public void setDistincts_pages_set(Set<String> distincts_pages_set) {
			this.distincts_pages_set = distincts_pages_set;
		}

		public Set<String> getDistincts_editors_set() {
			return distincts_editors_set;
		}

		public void setDistincts_editors_set(Set<String> distincts_editors_set) {
			this.distincts_editors_set = distincts_editors_set;
		}

		public Set<String> getDistincts_servers_set() {
			return distincts_servers_set;
		}

		public void setDistincts_servers_set(Set<String> distincts_servers_set) {
			this.distincts_servers_set = distincts_servers_set;
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
			return "AbsoluteValueShot2 [id=" + id + ", hour=" + hour + ", day=" + day + ", month=" + month + ", year="
					+ year + ", event_time=" + event_time + ", edits_data=" + edits_data + ", distincts_pages_set="
					+ distincts_pages_set + ", distincts_editors_set=" + distincts_editors_set
					+ ", distincts_servers_set=" + distincts_servers_set + ", smaller_origin=" + smaller_origin
					+ ", toString()=" + super.toString() + "]";
		}

    }