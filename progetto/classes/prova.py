from impl import *
# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("././data/collection-1.json")
col_dp.uploadData("././data/collection-2.json")

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)


print (grp_qp.getAllCanvases())
print (grp_qp.getDbPathOrUrl())