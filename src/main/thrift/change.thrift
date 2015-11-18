namespace java br.edu.ufcg.analytics.wikitrends.thrift

struct WikiMediaChange {
	1: required string id;
	2: required i32 type;
	3: required i32 rc_namespace;
	4: required string title;
	5: required string comment;
	6: required i32 timestamp;
	7: required string user;
	8: required bool bot;
	9: required string server_url;
	10: required string server_name;
	11: required string server_script_path;
	12: required string wiki;
	13: optional bool minor;
	14: optional bool patrolled;
	15: optional i32 length_old;
	16: optional i32 length_new;
	17: optional i32 revision_old;
	18: optional i32 revision_new;
	19: optional string log_id;
	20: optional string log_type;
	21: optional string log_action;
	22: optional string log_params;
	23: optional string log_action_comment;
}

struct Edit {
	1: required i32 id;
	2: required i32 type;
	3: required i32 rc_namespace;
	4: required string title;
	5: required string comment;
	6: required i32 timestamp;
	7: required string user;
	8: required bool bot;
	9: required string server_url;
	10: required string server_name;
	11: required string server_script_path;
	12: required string wiki;
	13: optional bool minor;
	14: optional bool patrolled;
	15: optional i32 length_old;
	16: optional i32 length_new;
	17: optional i32 revision_old;
	18: optional i32 revision_new;
	19: optional string log_id;
	20: optional string log_type;
	21: optional string log_action;
	22: optional string log_params;
	23: optional string log_action_comment;
}
