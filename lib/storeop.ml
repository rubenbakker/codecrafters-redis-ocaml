type 'a mutation_result = {
  store : 'a option;
  notify_with : Resp.t list;
  return : Resp.t;
}

type query_result = { return : Resp.t }
type 'a query_fn_t = 'a option -> query_result
