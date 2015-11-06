union UserID {
	1: string login,
}
union UserPropertyValue {
	1: boolean is_bot,
}
struct UserProperty {
	1: required PersonID id,
	2: required PersonPropertyValue property,
}

union PageID {
	1: string title,
}
union PagePropertyValue {
	1: i32 rc_namespace,
}
struct PageProperty {
	1: required PersonID id,
	2: required PersonPropertyValue property,
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
          4: required boolean minor,
          3: required i64 nonce,
}

struct LogEdge {
          1: required UserID person,
          2: required PageID page,
          3: required ServerID server,
          4: required boolean minor,
          3: required i64 nonce,
}

struct EditEdge {
          1: required UserID person,
          2: required PageID page,
          3: required ServerID server,
          4: required boolean minor,
          3: required i64 nonce,
}




union DataUnit {
	1: UserProperty user_property,
	2: PageProperty page_property,
	3: ServerProperty server_property,
	4: ChangeEdge change,
}
struct Pedigree {
	1: required i32 true_as_of_secs,
}
struct Data {
	1: required Pedigree pedigree,
	2: required DataUnit dataunit,
}

