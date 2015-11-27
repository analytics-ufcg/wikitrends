package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;
import java.util.UUID;

public class ResultTopIdiom implements Serializable {
		
	/**
	 *  SerialVersionUID to class ResultTopIdioms
	 *  
	 *  @since November 25, 2015
	 */
	private static final long serialVersionUID = -1144718268764679141L;
		
		private UUID id;
        private String idiom;
        private Integer count;

        public ResultTopIdiom(String idiom, Integer count) {
        	this.id = UUID.randomUUID();
            this.idiom = idiom;
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

		public String getIdiom() {
			return idiom;
		}

		public void setIdiom(String idiom) {
			this.idiom = idiom;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}

		@Override
		public String toString() {
			return "ResultTopIdioms [id=" + id + ", idiom=" + idiom + ", count=" + count + "]";
		}

    }