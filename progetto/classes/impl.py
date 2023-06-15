from pandas import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from rdflib import *
from sparql_dataframe import get

class collectionProcessor(object):
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
        
class triplestoreQueryProcessor(object):
    # def __init__(self, Canvas, Manifest, Collection, Canvases_Collection,Canvases_Manifest,Entities_Label,Manifest_Collections):
    #     self.canvas=get (endpoint, Canvas,True)
    #     self.Manifests=get (endpoint, Manifest,True)
    #     self.Collections=get (endpoint, Collection, True)
    #     self.Canvases_Collections=get (endpoint, Canvases_Collection, True)
    #     self.Canvases_Manifest=get (endpoint, Canvases_Manifest, True)
    #     self.Entities_Label=get (endpoint, Entities_Label,True)
    #     self.Manifest_Collections=get (endpoint, Manifest_Collections,True)
        
    def __init__(self):
     self.Canvas=DataFrame()
     self.Manifests=DataFrame()
     self.Collections=DataFrame()
     self.Canvases_Collections=DataFrame()
     self.Canvases_Manifest=DataFrame()
     self.Entities_Label=DataFrame()
     self.Manifest_Collections=DataFrame()
     self.endpoint=""

    def setDbPathOrUrl(self, path):
         self.endpoint = path
    
    def getAllCanvases(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {?canvas a iiif_prezi:Canvas.}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Canvas=df_sparq
        return self.Canvas
        
    def getAllCollections(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?collections where {?collections a iiif_prezi:Collection.}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Collections=df_sparq
        return self.Collections
        
    def getAllManifests(self):
        
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifests where {?manifests a iiif_prezi:Manifest.}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Manifests=df_sparq
        return self.Manifests
        
    def getCanvasesInCollections(self, collectionId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {<"""+str(collectionId)+"""> ast:items ?manifest.
                     ?manifest ast:items ?canvas}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Canvases_Collections=df_sparq
        return self.Canvases_Collections

    
    def getCanvasesInManifests(self, manifestId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {<"""+str(manifestId)+"""> ast:items ?canvas.}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Canvases_Manifest=df_sparq
        return self.Canvases_Manifest
        #ok
        
    def getEntitiesWithLabel(self, label:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select * where {?s rdfs:label \""""+str(label)+"""\"}       
        """
        #se non va rivedi escape characters
        df_sparq=get(self.endpoint,query,True)
        self.Entities_Label=df_sparq
        return self.Entities_Label
    
    def getManifetsInCollections (self, collectionId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifest where {<"""+str(collectionId)+"""> ast:items ?manifest.}
        """
        df_sparq=get(self.endpoint,query,True)
        self.Manifest_Collections=df_sparq
        return self.Manifest_Collections
    
