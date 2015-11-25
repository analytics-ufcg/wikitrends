package br.edu.ufcg.analytics.wikitrends.storage.results.types;

import java.io.Serializable;
import java.util.UUID;

public class ResultTopContentPage implements Serializable {
		
	/**
	 *  SerialVersionUID to class ResultTopContentPages
	 *  
	 *  @since November 25, 2015
	 */
	private static final long serialVersionUID = -229445761214486100L;
		
		private UUID id;
        private String content_page;
        private Integer count;

        public ResultTopContentPage(String content_page, Integer count) {
        	this.id = UUID.randomUUID();
            this.content_page = content_page;
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

		public String getContentPage() {
			return content_page;
		}

		public void setContentPage(String content_page) {
			this.content_page = content_page;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}

		@Override
		public String toString() {
			return "ResultTopContentPages [id=" + id + ", content_page=" + content_page + ", count=" + count + "]";
		}

    }