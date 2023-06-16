from impl import *
# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
# grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# col_dp = CollectionProcessor()
# col_dp.setDbPathOrUrl(grp_endpoint)
# col_dp.uploadData("././data/collection-1.json")
# col_dp.uploadData("././data/collection-2.json")

# grp_qp = TriplestoreQueryProcessor()
# grp_qp.setDbPathOrUrl(grp_endpoint)


# print (grp_qp.getAllCanvases())
# print (grp_qp.getDbPathOrUrl())

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("././data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("././data/metadata.csv")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

print (rel_qp.getAnnotationsWithBody("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))

