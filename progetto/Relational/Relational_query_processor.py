from pandas import DataFrame
from relational import annotations_ids
from classes import Image
from sqlite3 import connect 
from pandas import read_sql
class Relational_query_processor(object):
    def __init__(self, annotations, Image, entities, query_processor):
        self.Annotation = annotations
        self.Images = Image
        self.entities = entities
        self.query_processor = query_processor

    def getAllAnnotations(self):
        return annotations
    
        
    def getAllImages(self):
        return Image

    # def getAnnotationsWithBody(body):
    #     value = body
    #     with connect("annotations_metadata.db") as con:
    #         query = "SELECT * FROM Annotations WHERE Body = value"
    #         results = read_sql(query, con)
    #     print(results)
    print(annotations_ids.columns)
    
# print(Relational_query_processor.getAnnotationsWithBody("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))
    #def getAnnotationsWithBodyAndTarget(self):
        
    #def getEntitiesWithCreator(self):
        
    #def getEntitiesWithTitle(self):
        