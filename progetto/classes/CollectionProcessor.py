from rdflib import URIRef, Literal, RDF
from pandas import read_csv, Series
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from rdflib import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

class CollectionProcessor(object):
    def __init__(self):
        self.path=""
        self.endpoint=""
        self.iiif_prezi = Namespace("http://iiif.io/api/presentation/3#")
        self.ast = Namespace("http://www.w3.org/ns/activitystreams#")
        self.rdf= Namespace("http://www.w3.org/2000/01/rdf-schema#")

    def setDbPathOrUrl (self, url:str):
        self.endpoint=url

    def uploadData (self, path:str):
         
        def fill (father, json_doc):
            for i in json_doc:
                if i == "id":
                    id=URIRef(json_doc[i])
                    if father:
                        k_graph.add((father, Item, id))
                    
                if i == "type":
                    k_graph.add((id, RDF.type, URIRef(self.iiif_prezi+json_doc[i])))
                if i == "label":
                    for j in json_doc[i]:
                        for k in json_doc[i][j]:
                            k_graph.add((id, Label, Literal(k)))
                if i == "items":
                    for j in json_doc[i]:
                        fill (id, j)
                else: pass

        self.path=path
        
        with open(path, "r", encoding="utf-8") as f:
            json_doc1 = load(f)

        # with open("./data/collection-2.json", "r", encoding="utf-8") as f:
        #     json_doc2 = load(f)

        k_graph= Graph()

        k_graph.bind("iiif_prezi", self.iiif_prezi)
        k_graph.bind("ast", self.ast)
        k_graph.bind("rdf", self.rdf)

        Item = URIRef (self.ast+"items")
        Label=URIRef (self.rdf+"label")

        fill(None, json_doc1)
                

        store = SPARQLUpdateStore()

        # The URL of the SPARQL endpoint is the same URL of the Blazegraph
        # instance + '/sparql'
        

        # It opens the connection with the SPARQL endpoint instance
        store.open((self.endpoint, self.endpoint))

        #delete_query="""DELETE WHERE {?s ?p ?o.}"""

        for triple in k_graph.triples((None, None, None)):
            store.add(triple)
            
        #store.update(delete_query)
        # Once finished, remeber to close the connection
        store.close()
        