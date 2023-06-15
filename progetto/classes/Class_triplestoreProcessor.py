from pandas import DataFrame
#from progetto import classes
from  sparql_dataframe import get

endpoint = "http://127.0.0.1:9999/blazegraph/sparql"


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self, Canvas, Manifest, Collection, Canvases_Collection,Canvases_Manifest,Entities_Label,Manifest_Collections):
        self.canvas=get (endpoint, Canvas,True)
        self.Manifests=get (endpoint, Manifest,True)
        self.Collections=get (endpoint, Collection, True)
        self.Canvases_Collections=get (endpoint, Canvases_Collection, True)
        self.Canvases_Manifest=get (endpoint, Canvases_Manifest, True)
        self.Entities_Label=get (endpoint, Entities_Label,True)
        self.Manifest_Collections=get (endpoint, Manifest_Collections,True)
        
    def getAllCanvases(self):
        query="""
        "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        "PREFIX schema: <https://schema.org/>
        SELECT ?Canvas
        WHERE {?s rdf:type iif: Canvas.
               }
        """
        df_sparq=get(endpoint,query,True)
        self.canvas=df_sparq
        return self.canvas
        
    def getAllCollections(self):
        
        return self.Collections
        
    def getAllManifests(self):
        return self.Manifests
        
    def getCanvasesInCollections(collectionId):


    
    def getCanvasesInManifests(manifestId):
        
    def getEntitiesWithLabel(label):
    
    def ManifetsInCollections (collectionId):
        