from pandas import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from rdflib import *
from sparql_dataframe import get
from sqlite3 import connect
import pandas as pd
import re

class Processor (object):
    def __init__(self):
        self.dbPathOrUrl=""

    def setDbPathOrUrl (self, url:str):
        self.dbPathOrUrl=url

    def getDbPathOrUrl (self):
        return self.dbPathOrUrl

class QueryProcessor (Processor):
    def __init__(self):
        self.Entity=DataFrame()
    
    def getEntityById(self, id:str):

        if ".db" in self.dbPathOrUrl:
            def find_type (id):
                with connect(self.dbPathOrUrl) as con:
                    query = """SELECT *
                            FROM Collection
                            WHERE Collection.id=?"""
                result = read_sql(query, con, params=(id,))
                if len(result)>0: 
                    query = """select *
                            from Collection
                            LEFT JOIN Creator ON Creator.InternalID=Collection.internalID
                            where Collection.id=?"""
                    result = read_sql(query, con, params=(id,))
                    return result.loc[:, ~result.columns.duplicated()]
                
                with connect(self.dbPathOrUrl) as con:
                    query = """SELECT *
                            FROM Canvas
                            WHERE Canvas.id=?"""
                result = read_sql(query, con, params=(id,))
                if len(result)>0: 
                    query = """select *
                            from Canvas
                            LEFT JOIN Creator ON Creator.InternalID=Canvas.internalID
                            where Canvas.id=?"""
                    result = read_sql(query, con, params=(id,))
                    return result.loc[:, ~result.columns.duplicated()]

                with connect(self.dbPathOrUrl) as con:
                    query = """SELECT *
                            FROM Manifest
                            WHERE Manifest.id=?"""
                result = read_sql(query, con, params=(id,))
                if len(result)>0: 
                    query = """select distinct *
                            from Manifest
                            LEFT JOIN Creator ON Creator.InternalID=Manifest.internalID
                            where Manifest.id=?"""
                    result = read_sql(query, con, params=(id,))
                    return result.loc[:, ~result.columns.duplicated()]

                with connect(self.dbPathOrUrl) as con:
                    query = """SELECT *
                            FROM Image
                            WHERE Image.image_url=?"""
                result = read_sql(query, con, params=(id,))
                if len(result)>0: return result.loc[:, ~result.columns.duplicated()]

                with connect(self.dbPathOrUrl) as con:
                    query = """SELECT *
                            FROM Annotation
                            WHERE Annotation.id=?"""
                result = read_sql(query, con, params=(id,))
                if len(result)>0: return result.loc[:, ~result.columns.duplicated()]

                return None
            
            return find_type(id)
                    
        else:
            query="""
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX ast: <http://www.w3.org/ns/activitystreams#>
                    PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

                    select ?label ?items where {<"""+str(id)+"""> rdfs:label ?label.
                    optional {<"""+str(id)+"""> ast:items ?items.}
                    }
                    """
            df_sparq=get(self.dbPathOrUrl,query,True)
            return df_sparq


class AnnotationProcessor (Processor):
    def __init__(self):
        self.Annotation=DataFrame()
        self.Image=DataFrame()

    def uploadData (self, path:str):

        def extract_id(s):               #aggiunto
            pattern = re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
            if pattern not in s:
                return None
            else:
                return pattern
            pass

        self.Annotation = read_csv(path, keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})
        self.Image = self.Annotation[["body"]]
        self.Image.insert(0, "imageID", Series(self.Annotation["body"].apply(extract_id), dtype="string"))
        self.Image = self.Image.rename(columns={"body":"image_url"})
            # annotations_j = annotations_j.rename(columns={"internalID_x":"internalID", "id_x":"id", "internalID_y":"target"})

        # df_joined = merge(self.Annotation, self.Image, left_on="body", right_on="image_url")
        # self.Annotation = df_joined[["id", "imageID", "target", "motivation"]]
        # self.Annotation = self.Annotation.rename(columns={"imageID":"body"})

        with connect(self.dbPathOrUrl) as con:
            self.Image.to_sql("Image", con, if_exists="replace", index=False)
            self.Annotation.to_sql("Annotation", con, if_exists="replace", index=False)
            con.commit()

class MetadataProcessor (Processor):
    def __init__(self):
        self.Collection=DataFrame()
        self.Manifest=DataFrame()
        self.Canvas=DataFrame()
        self.Metadata=DataFrame()
        self.Creator=DataFrame()
        self.Collection_items=DataFrame()
        self.Manifest_items=DataFrame()

    def uploadData (self, path:str):
        
        def replacer(j):
            return j.replace('""', '"')
        
        
        def extract_id(s):               #aggiunto
            pattern = re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
            if pattern not in s:
                return None
            else:
                return pattern
            pass

        def is_part_of(x, y, type_1, type_2):
            x = x.strip("/" + type_1)
            y = y.strip("/" + type_2)
            x = x.split("/")
            y = y.split("/")
            if type_2 == "manifest":
                if y[1] in x[0]:
                    return True
            if type_2 == "canvas":
                if y[1] in x[1] and y[0] in x[0]:
                    return True

        
        self.Metadata = read_csv(path, keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})
        
        self.Metadata.insert(0, "internalID", Series(self.Metadata["id"].apply(extract_id), dtype="string"))


        self.Creator = self.Metadata[["creator", "internalID"]] 
        

        collection_id = ""
        manifest_id = ""
        canvas_id = ""
        for word, row in self.Metadata.iterrows():
            if "collection" in row["id"]:
                collection_id = row["internalID"]
                try:
                    self.Collection = self.Collection._append(row[["id", "internalID", "title"]])
                except (FutureWarning, AttributeError):
                    self.Collection = self.Collection.append(row[["id", "internalID", "title"]])

            if "manifest" in row["id"]:
                manifest_id = row["internalID"]
                try:
                    self.Manifest = self.Manifest._append(row[["id", "internalID", "title"]])
                except (FutureWarning, AttributeError):
                    self.Manifest = self.Manifest.append(row[["id", "internalID", "title"]])

            if "canvas" in row["id"]:
                try:
                    self.Canvas = self.Canvas._append(row[["id", "internalID", "title"]])
                except (FutureWarning, AttributeError):
                    self.Canvas = self.Canvas.append(row[["id", "internalID", "title"]])

        
        for index, row in self.Collection.iterrows():
            # Iterate over rows in 'metadata' DataFrame
            for index_2, row_2 in self.Metadata.iterrows():
                if "manifest" in row_2["id"]:
                    # print (row["id"], row_2["id"], is_part_of(row["internalID"], row_2["internalID"], "collection", "manifest"))
                    if is_part_of(row["internalID"], row_2["internalID"], "collection", "manifest"):
                        # Create a temporary DataFrame with the desired values
                        temp_df = DataFrame({"collection_id": [row["internalID"]], "manifest_id": [row_2["internalID"]]})
                        # Append the temporary DataFrame to 'collection_items'
                        try:
                            self.Collection_items = self.Collection_items._append(temp_df)
                        except (FutureWarning, AttributeError):
                            self.Collection_items = self.Collection_items.append(temp_df)                            
        self.Collection_items = self.Collection_items.reset_index(drop=True)

        # Iterate over rows in 'manifest' DataFrame
        for index, row in self.Manifest.iterrows():
            # Iterate over rows in 'metadata' DataFrame
            for index_2, row_2 in self.Metadata.iterrows():
                if "canvas" in row_2["id"]:
                    if is_part_of(row["internalID"], row_2["internalID"], "manifest", "canvas"):
                        # Create a temporary DataFrame with the desired values
                        temp_df = DataFrame({"manifest_id": [row["internalID"]], "canvas_id": [row_2["internalID"]]})
                        # Append the temporary DataFrame to 'manifest_items'
                        try:
                            self.Manifest_items = self.Manifest_items._append(temp_df)
                        except (FutureWarning, AttributeError):
                            self.Manifest_items = self.Manifest_items.append(temp_df)
        self.Manifest_items = self.Manifest_items.reset_index(drop=True)

        with connect(self.dbPathOrUrl) as con:
            self.Creator.to_sql("Creator", con, if_exists="replace", index=False)
            self.Collection.to_sql("Collection", con, if_exists="replace", index=False)
            self.Manifest.to_sql("Manifest", con, if_exists="replace", index=False)
            self.Canvas.to_sql("Canvas", con, if_exists="replace", index=False)
            self.Collection_items.to_sql("Collection_Items", con, if_exists="replace", index=False)
            self.Manifest_items.to_sql("Manifest_Items", con, if_exists="replace", index=False)
            con.commit()

        # print(self.Collection)
        # print(self.Manifest)
        # print(self.Canvas)
        # print(self.Creator)
        # print(self.Collection_items)
        # print(self.Manifest_items)

    
class CollectionProcessor(Processor):
    def __init__(self):
        self.path=""
        self.iiif_prezi = Namespace("http://iiif.io/api/presentation/3#")
        self.ast = Namespace("http://www.w3.org/ns/activitystreams#")
        self.rdf= Namespace("http://www.w3.org/2000/01/rdf-schema#")

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

        # The URL of the SPARQL dbPathOrUrl is the same URL of the Blazegraph
        # instance + '/sparql'
        

        # It opens the connection with the SPARQL dbPathOrUrl instance
        store.open((self.dbPathOrUrl, self.dbPathOrUrl))

        #delete_query="""DELETE WHERE {?s ?p ?o.}"""

        for triple in k_graph.triples((None, None, None)):
            store.add(triple)
            
        #store.update(delete_query)
        # Once finished, remeber to close the connection
        store.close()
        
class TriplestoreQueryProcessor(QueryProcessor):
    # def __init__(self, Canvas, Manifest, Collection, Canvases_Collection,Canvases_Manifest,Entities_Label,Manifest_Collections):
    #     self.canvas=get (dbPathOrUrl, Canvas,True)
    #     self.Manifests=get (dbPathOrUrl, Manifest,True)
    #     self.Collections=get (dbPathOrUrl, Collection, True)
    #     self.Canvases_Collections=get (dbPathOrUrl, Canvases_Collection, True)
    #     self.Canvases_Manifest=get (dbPathOrUrl, Canvases_Manifest, True)
    #     self.Entities_Label=get (dbPathOrUrl, Entities_Label,True)
    #     self.Manifest_Collections=get (dbPathOrUrl, Manifest_Collections,True)
        
    def __init__(self):
     self.Canvas=DataFrame()
     self.Manifests=DataFrame()
     self.Collections=DataFrame()
     self.Canvases_Collections=DataFrame()
     self.Canvases_Manifest=DataFrame()
     self.Entities_Label=DataFrame()
     self.Manifest_Collections=DataFrame()
     self.dbPathOrUrl=""
    
    def getAllCanvases(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {?canvas a iiif_prezi:Canvas.}
        """
        df_sparq=get(self.dbPathOrUrl,query,True)
        self.Canvas=df_sparq
        return self.Canvas
        
    def getAllCollections(self):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?collections where {?collections a iiif_prezi:Collection.}
        """
        df_sparq=get(self.dbPathOrUrl,query,True)
        self.Collections=df_sparq
        return self.Collections
        
    def getAllManifests(self):
        
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifest ?label where {?manifest a iiif_prezi:Manifest;
                                                rdfs:label ?label}
        """
        df_sparq=get(self.dbPathOrUrl,query,True)
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
        df_sparq=get(self.dbPathOrUrl,query,True)
        self.Canvases_Collections=df_sparq
        return self.Canvases_Collections

    
    def getCanvasesInManifests(self, manifestId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?canvas where {<"""+str(manifestId)+"""> ast:items ?canvas.}
        """
        df_sparq=get(self.dbPathOrUrl,query,True)
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
        df_sparq=get(self.dbPathOrUrl,query,True)
        self.Entities_Label=df_sparq
        return self.Entities_Label
    
    def getManifetsInCollections (self, collectionId:str):
        query="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ast: <http://www.w3.org/ns/activitystreams#>
        PREFIX iiif_prezi: <http://iiif.io/api/presentation/3#>

        select ?manifest where {<"""+str(collectionId)+"""> ast:items ?manifest.}
        """
        df_sparq=get(self.dbPathOrUrl,query,True)
        self.Manifest_Collections=df_sparq
        return self.Manifest_Collections
    
class RelationalQueryProcessor (QueryProcessor):

    def __init__(self):
        self.Annotation = DataFrame()
        self.Images = DataFrame()
        self.entities = DataFrame()
        # self.query_processor = query_processor
    def replacer(self, j):
        return j.replace('""', '"')
    
    def extract_id(self, s):               #aggiunto
            pattern = re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
            if pattern not in s:
                return s
            else:
                return pattern
            pass
    
    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = """SELECT *
            FROM Annotation"""
        result = read_sql(query, con)
        return result
    
        
    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = """SELECT image_url
            FROM Image"""
        results = read_sql(query, con)
        return results

    def getAnnotationsWithBody(self,body):
        body = self.extract_id(body)
        with connect(self.dbPathOrUrl) as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE body = ?"""
            results = read_sql(query, con, params=(body,))
        return results
    
   
    def getAnnotationsWithBodyAndTarget(self, body, target):
        body = self.extract_id(body)
        with connect(self.dbPathOrUrl) as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE body = ? AND target =?"""
            results = read_sql(query, con, params=(body, target))
        return results

    def getAnnotationsWithTarget(self, target):
        with connect(self.dbPathOrUrl) as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE target =?"""
        result = read_sql(query, con, params=(target,))
        return result

        
    
    
    def getEntitiesWithCreator(self,creator):
        with connect(self.dbPathOrUrl) as con:
            query = """ SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
                    FROM Creator
                    LEFT JOIN Collection ON Creator.internalID = Collection.internalID
                    LEFT JOIN Manifest ON Creator.internalID = Manifest.internalID
                    LEFT JOIN Canvas ON Creator.internalID = Canvas.internalID
                    WHERE creator=?
                    """
        result = read_sql(query, con, params=(creator,))
        return result
    
    def getEntitiesWithTitle(self, title):
        print(title)
        title = self.replacer(title)
        print(title)
        with connect(self.dbPathOrUrl) as con:
            query = """
                    SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
                    FROM Creator
                    LEFT JOIN Collection ON Creator.internalID = Collection.internalID
                    LEFT JOIN Manifest ON Creator.internalID = Manifest.internalID
                    LEFT JOIN Canvas ON Creator.internalID = Canvas.internalID
                    WHERE ? IN (Collection.title, Manifest.title, Canvas.title)
                    """
        result = read_sql(query, con, params=(title,))
        return result


class GenericQueryProcessor (object):
    def __init__ (self):
        self.queryProcessors=list()
    
    def addQueryProcessor(self, processor):
        self.queryProcessors.append(processor)

    def cleanQueryProcessor(self):
        self.queryProcessor=list()

    def getAllAnnotations(self):
        result = list()
        for i in self.queryProcessors:
            if type(i) == RelationalQueryProcessor:
                annotations = i.getAllAnnotations()
                for j, row in annotations.iterrows():
                    
                    result.append(Annotation(str(row[annotations.columns.get_loc("id")]), str(row[annotations.columns.get_loc("motivation")]), str(row[annotations.columns.get_loc("target")]), str(row[annotations.columns.get_loc("body")])))
                return result
            if type(i) == TriplestoreQueryProcessor:
                pass
            
    def getImagesAnnotatingCanvas(self):
        for i in self.queryProcessors:
            if type(i) == RelationalQueryProcessor:
                result = list()
    
    def getAllManifest(self):
        result = list()
        temp = DataFrame()
        temp_2 = DataFrame()
        id = ""
        label = ""
        for i in self.queryProcessors:
            if type(i) == TriplestoreQueryProcessor:
                #print ("oktrpl")
                temp = i.getAllManifests()
        for i in self.queryProcessors:
            if type(i) == RelationalQueryProcessor:
                #print ("okrel")
                for index,row in temp.iterrows():
                    # id = row[temp.columns.get_loc("manifest")]
                    # label = row[temp.columns.get_loc("label")]
                    temp_2 = i.getEntityById(row[temp.columns.get_loc("manifest")])
                    result.append(Manifest(str(row[temp.columns.get_loc("manifest")]), str(temp_2["creator"].values[0]).split(";"), str(row[temp.columns.get_loc("label")]), str(temp_2[["title"]])))
        
        return result
 
                
    # def getAllCanvas(self):
    #     for i in self.queryProcessor:
    #         if type(i) == RelationalQueryProcessor:
    #             result = list()
    #             for j in .iterrows():
    #                 result.append(Canvas(str(canvas([["id"]], str(canvas[["title"]])))))
    #         if type(i) == TriplestoreQueryProcessor:
                

class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

    def getId(self):
        return self.id


class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation, target, body):
        super().__init__(id)
        self.motivation = motivation
        self.target = target
        self.body = body
    def getMotivation(self):
        return self.motivation
    
class Image(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)

class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label, title, creators:list):
        super().__init__(id)
        self.creators = creators
        self.label = label
        self.title = title
    def getLabel(self):
        return self.label
    def getTitle(self):
        return self.title
    def getCreators(self):
        return self.creators

class Collection(EntityWithMetadata):
    def __init__(self, id, creators, label, title):
        super().__init__(id, label, title, creators)
        # super().__init__(creator)
        # super().__init__(label)
        # super().__init__(title)
        self.label = label
        self.items = list()       

class Manifest(EntityWithMetadata):
    def __init__(self, id, creators:list, label, title):
        super().__init__(id, label, title, creators)
        # super().__init__(creator)
        # super().__init__(label)
        # super().__init__(title)
        self.label = label 
        self.items = list()
    def getItems():
        pass

class Canvas(EntityWithMetadata):
    def __init__(self, id, creators, label, title):
        super().__init__(id, label, title, creators)
        # super().__init__(creator)
        # super().__init__(label)
        # super().__init__(title)
        self.label = label