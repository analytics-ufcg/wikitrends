package br.edu.ufcg.analytics.wikitrends.storage.results.types;

import java.io.Serializable;
import java.util.UUID;

/**
 * 
 * Class that represents a stored Absolute Values Shot into the results layer.
 * 
 * @author Guilherme Gadelha
 *
 */
public class ResultAbsoluteValuesShot implements Serializable {
		
		private static final long serialVersionUID = -6443116682782406267L;

		private UUID id;
        
        private Long all_edits;
        private Long minor_edits;
        private Long average_size;

        private Long distinct_pages_count;
		private Integer distinct_editors_count;
		private Integer distinct_servers_count;

		private Long smaller_origin;

		public ResultAbsoluteValuesShot(Long all_edits,
										Long minor_edits,
										Long average_size,
										Long distinct_pages_count, 
										Integer distinct_editors_count, 
										Integer distinct_servers_count, 
										Long smaller_origin) {
            
        	this.id = UUID.randomUUID();
            this.all_edits = all_edits;
            this.minor_edits = minor_edits;
            this.average_size = average_size;
            this.distinct_pages_count = distinct_pages_count;
            this.distinct_editors_count = distinct_editors_count;
            this.distinct_servers_count = distinct_servers_count;
            this.smaller_origin = smaller_origin;

		}
        
		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
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

		public Long getDistinct_pages_count() {
			return distinct_pages_count;
		}

		public void setDistinct_pages_count(Long distinct_pages_count) {
			this.distinct_pages_count = distinct_pages_count;
		}

		public Integer getDistinct_editors_count() {
			return distinct_editors_count;
		}

		public void setDistinct_editors_count(Integer distinct_editors_count) {
			this.distinct_editors_count = distinct_editors_count;
		}

		public Integer getDistinct_servers_count() {
			return distinct_servers_count;
		}

		public void setDistinct_servers_count(Integer distinct_servers_count) {
			this.distinct_servers_count = distinct_servers_count;
		}

		public Long getSmaller_origin() {
			return smaller_origin;
		}

		public void setSmaller_origin(Long smaller_origin) {
			this.smaller_origin = smaller_origin;
		}

		@Override
		public String toString() {
			return "ResultAbsoluteValuesShot [id=" + id + ", all_edits=" + all_edits + ", minor_edits=" + minor_edits
					+ ", average_size=" + average_size + ", distincts_pages_count=" + distinct_pages_count
					+ ", distincts_editors_count=" + distinct_editors_count + ", distincts_servers_count="
					+ distinct_servers_count + ", smaller_origin=" + smaller_origin + ", toString()="
					+ super.toString() + "]";
		}
    }