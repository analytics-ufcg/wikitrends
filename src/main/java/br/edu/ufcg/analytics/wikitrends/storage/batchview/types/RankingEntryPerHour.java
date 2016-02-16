package br.edu.ufcg.analytics.wikitrends.storage.batchview.types;

import java.io.Serializable;
import java.time.LocalDateTime;

public class RankingEntryPerHour implements Serializable {
	
	/**
	 *  SerialVersionUID to class TopClass
	 *  
	 *  @since December 1, 2015	
	 */
	private static final long serialVersionUID = 4549928096222419333L;
		
		private Integer hour;
    	private Integer day;
    	private Integer month;
    	private Integer year;
    	
        private String name;
        private Long count;

        public RankingEntryPerHour(String name, Long count, Integer year, Integer month, Integer day, Integer hour) {
        	setName(name);
        	setCount(count);
            setYear(year);
    		setMonth(month);
    		setDay(day);
    		setHour(hour);
        }
        
        public RankingEntryPerHour(String name, Long count, LocalDateTime date) {
        	this(name, count, date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour());
        }

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}


		public Long getCount() {
			return count;
		}


		public void setCount(Long count) {
			this.count = count;
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
		
		@Override
		public String toString() {
			return "TopClass [hour=" + hour + ", day=" + day + ", month=" + month + ", year=" + year + ", name=" + name
					+ ", count=" + count + "]";
		}
    }