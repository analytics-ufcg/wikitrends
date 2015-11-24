package br.edu.ufcg.analytics.wikitrends.storage.serving.types;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;

public class TopClass implements Serializable {
		private static final long serialVersionUID = 2644747318393169105L;
		
		private UUID id;
		private Integer hour;
    	private Integer day;
    	private Integer month;
    	private Integer year;
        private Date event_time;
        private Map<String, Integer> data;

        public TopClass(Map<String, Integer> data) {
        	this.id = UUID.randomUUID();
            this.data = data;

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

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
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
			
			DateTime date = new DateTime(event_time);
			setYear(date.getYear());
    		setMonth(date.getMonthOfYear());
    		setDay(date.getDayOfMonth());
    		setHour(date.getHourOfDay());
		}

		public Map<String, Integer> getData() {
			return data;
		}

		public void setData(Map<String, Integer> data) {
			this.data = data;
		}
		
		@Override
		public String toString() {
			return "TopClass2 [id=" + id + ", hour=" + hour + ", day=" + day + ", month=" + month + ", year=" + year
					+ ", event_time=" + event_time + ", data=" + data + "]";
		}
    }