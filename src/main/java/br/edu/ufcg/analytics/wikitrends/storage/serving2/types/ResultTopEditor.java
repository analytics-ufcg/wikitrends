package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;
import java.util.UUID;

public class ResultTopEditor implements Serializable {
		
	/**
	 *  SerialVersionUID to class ResultTopEditors
	 *  
	 *  @since November 25, 2015
	 */
	private static final long serialVersionUID = 9168875187154281613L;
		
		private UUID id;
        private String editor;
        private Integer count;

        public ResultTopEditor(String editor, Integer count) {
        	this.id = UUID.randomUUID();
            this.editor = editor;
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

		public String getEditor() {
			return editor;
		}

		public void setEditor(String editor) {
			this.editor = editor;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}

		@Override
		public String toString() {
			return "ResultTopClass [id=" + id + ", editor=" + editor + ", count=" + count + "]";
		}

    }