from CollectionProcessor import collectionProcessor
from TriplestoreQueryProcessor import triplestoreQueryProcessor


# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = collectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("././data/collection-1.json")
col_dp.uploadData("././data/collection-2.json")

grp_qp = triplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

result_q1 = grp_qp.getAllCanvases()

print (result_q1)