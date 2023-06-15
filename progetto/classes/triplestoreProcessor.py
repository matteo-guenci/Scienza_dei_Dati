from pandas import DataFrame
#from progetto import classes
from rdflib import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get


endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

#class TriplestoreQueryProcessor(QueryProcessor):
class TriplestoreQueryProcessor(object):
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
     endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
    
    def getAllCanvases(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {?canvas a iiif_prezi:Canvas.}
        """
        df_sparq=get(endpoint,query,True)
        self.Canvas=df_sparq
        return self.Canvas
        
    def getAllCollections(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?collections where {?collections a iiif_prezi:Collection.}
        """
        df_sparq=get(endpoint,query,True)
        self.Collections=df_sparq
        return self.Collections
        
    def getAllManifests(self):
        
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifests where {?manifests a iiif_prezi:Manifest.}
        """
        df_sparq=get(endpoint,query,True)
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
        df_sparq=get(endpoint,query,True)
        self.Canvases_Collections=df_sparq
        return self.Canvases_Collections

    
    def getCanvasesInManifests(self, manifestId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {<"""+str(manifestId)+"""> ast:items ?canvas.}
        """
        df_sparq=get(endpoint,query,True)
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
        df_sparq=get(endpoint,query,True)
        self.Entities_Label=df_sparq
        return self.Entities_Label
    
    def getManifetsInCollections (self, collectionId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifest where {<"""+str(collectionId)+"""> ast:items ?manifest.}
        """
        df_sparq=get(endpoint,query,True)
        self.Manifest_Collections=df_sparq
        return self.Manifest_Collections
    
prova=TriplestoreQueryProcessor()


print (prova.getAllCanvases())