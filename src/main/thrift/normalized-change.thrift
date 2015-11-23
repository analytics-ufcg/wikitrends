namespace java br.edu.ufcg.analytics.wikitrends.thrift.normalized

union UserID {
	1: string login,
}
union UserPropertyValue {
	1: bool is_bot,
}
struct UserProperty {
	1: required UserID id,
	2: required UserPropertyValue property,
}

union PageID {
	1: string title,
}
union PagePropertyValue {
	1: i32 rc_namespace,
}
struct PageProperty {
	1: required PageID id,
	2: required PagePropertyValue property,
}

union ServerID {
	1: string wiki,
}
union ServerPropertyValue {
	1: string server_url,
	2: string server_name,
	3: string server_script_path,
}
struct ServerProperty {
	1: required ServerID id,
	2: required ServerPropertyValue property,
}



struct EditEdge {
          1: required UserID person,
          2: required PageID page,
          3: required ServerID server,
          4: required bool minor,
          5: required i64 nonce,
}

struct LogEdge {
          1: required UserID person,
          2: required PageID page,
          3: required ServerID server,
          4: required bool minor,
          5: required i64 nonce,
}

struct ExternalEdge {
          1: required UserID person,
          2: required PageID page,
          3: required ServerID server,
          4: required bool minor,
          5: required i64 nonce,
}




union DataUnit {
	1: UserProperty user_property,
	2: PageProperty page_property,
	3: ServerProperty server_property,
	4: EditEdge edit,
	5: LogEdge log,
	6: ExternalEdge external,
}
union Pedigree {
	1: required i32 true_as_of_secs,
}
struct Data {
	1: required Pedigree pedigree,
	2: required DataUnit dataunit,
}

