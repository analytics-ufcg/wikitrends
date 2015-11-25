package br.edu.ufcg.analytics.wikitrends.storage.results.types;

import java.io.Serializable;
import java.util.UUID;

public class ResultTopPage implements Serializable {
		
	/**
	 *  SerialVersionUID to class ResultTopPages
	 *  
	 *  @since November 25, 2015
	 */
	private static final long serialVersionUID = 8569971358215494608L;
		
		private UUID id;
        private String page;
        private Integer count;

        public ResultTopPage(String page, Integer count) {
        	this.id = UUID.randomUUID();
            this.page = page;
            this.count = count;
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

		public String getPage() {
			return page;
		}

		public void setPage(String page) {
			this.page = page;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}

		@Override
		public String toString() {
			return "ResultTopPages [id=" + id + ", page=" + page + ", count=" + count + "]";
		}

    }