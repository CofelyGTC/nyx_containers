from cachetools import cached, LRUCache, TTLCache


@cached(cache=TTLCache(maxsize=1024, ttl=300))
def get_targets(es, cogen_name='lutosa'):
    obj_param = es.get(index="cogen_parameters", id=cogen_name)
    return obj_param['_source']['targets']