from impl import *
# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("data/collection-1.json")
col_dp.uploadData("data/collection-2.json")

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)


# print (grp_qp.getAllCanvases())
# print (grp_qp.getDbPathOrUrl())

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

#print(rel_qp.getEntitiesWithTitle('Raimondi, Giuseppe. Quaderno manoscritto, "Caserma Scalo : 1930-1968"'))
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)
# result = generic.getAllAnnotations()
# for i in result:
#     print(i.getMotivation())

# result2=generic.getAllCollections()
# for i in result2:
#     print(i.getCreators())
# result=generic.getCanvasesInManifest("https://dl.ficlit.unibo.it/iiif/2/28429/manifest")
# for i in result:
#     print (i.getId())
# print(rel_qp.getEntityById("https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0001-image"))
result=generic.getEntitiesWithLabel('Il Canzoniere')

for i in result:
    print(type(i))
    print (i.getId())

