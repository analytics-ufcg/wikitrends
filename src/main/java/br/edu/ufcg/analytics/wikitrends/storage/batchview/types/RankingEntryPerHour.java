package br.edu.ufcg.analytics.wikitrends.storage.serving1.types;

import java.io.Serializable;

public class TopClass implements Serializable {
	
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

        public TopClass(String name, Long count, Integer year, Integer month, Integer day, Integer hour) {
        	setName(name);
        	setCount(count);
            setYear(year);
    		setMonth(month);
    		setDay(day);
    		setHour(hour);
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