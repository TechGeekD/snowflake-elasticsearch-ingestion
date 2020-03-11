
from elastic import es
from request_es import res
from utils import Pretty
from constants import ES_INDEX

# for hit in res['hits']['hits']:
#     res = es.delete(index=ES_INDEX, id=hit.get('_id'))
#     result = res['result']
#     id = hit.get('_id')
#     print(f'{result}: {id}')

res = es.indices.delete(index=ES_INDEX)
print('********************* print_delete_idx **************************')
print(Pretty(res).print())