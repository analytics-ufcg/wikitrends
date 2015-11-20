package br.edu.ufcg.analytics.wikitrends.storage.serving.types;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.http.annotation.Obsolete;

@Obsolete
public class TopClass implements Serializable {
		private static final long serialVersionUID = 2644747318393169105L;
		
		private UUID id;
    	private Date date_event;
        private Integer hour;
        private Date event_time;
        private Map<String, Integer> data;

        public TopClass() { }

        public TopClass(UUID id, Date date_event, Integer hour, Date event_time, Map<String, Integer> data) {
            this.id = id;
        	this.date_event = date_event;
            this.hour = hour;
            this.event_time = event_time;
            this.data = data;
        }
        
        public void setid(UUID id) {
        	this.id = id;
        }
        
        public UUID getid() {
        	return id;
        }

		public Integer getHour() {
			return hour;
		}

		public void setHour(Integer hour) {
			this.hour = hour;
		}
		
		public Date getDate_event() {
			return date_event;
		}
		
		public void setDate_event(Date date_event) {
			this.date_event = date_event;
		}

		
		public Date getEvent_time() {
			return event_time;
		}

		public void setEvent_time(Date event_time) {
			this.event_time = event_time;
		}

		public Map<String, Integer> getData() {
			return data;
		}

		public void setData(Map<String, Integer> data) {
			this.data = data;
		}
		
		@Override
        public String toString() {
            return MessageFormat.format("TopClass: '{'ID={0},Hour={1},Data={2},Date={3},EventTime={4}'}'", id, hour, data, date_event, event_time);
        }
    }