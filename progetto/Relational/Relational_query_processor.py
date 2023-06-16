from pandas import DataFrame
from classes import Image
from sqlite3 import connect 
from pandas import read_sql
import relational_2
class Relational_query_processor(object):
    def __init__(self):
        self.Annotation = DataFrame()
        self.Images = DataFrame()
        self.entities = DataFrame()
        # self.query_processor = query_processor

    def getAllAnnotations():
        with connect("annotations_metadata_2.db") as con:
            query = """SELECT *
            FROM Annotation"""
        result = read_sql(query, con)
        return result
    
        
    def getAllImages():
        with connect("annotations_metadata_2.db") as con:
            query = """SELECT image_url
            FROM Image"""
        results = read_sql(query, con)
        return results

    def getAnnotationsWithBody(body):
        body = body.replace('""', '"')
        body = relational_2.extract_id(body)
        with connect("annotations_metadata_2.db") as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE body = ?"""
            results = read_sql(query, con, params=(body,))
        return results
    
   
    def getAnnotationsWithBodyAndTarget(body, target):
        body = body.replace('""', '"')
        target = target.replace('""', '"')
        body = relational_2.extract_id(body)
        with connect("annotations_metadata_2.db") as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE body = ? AND target =?"""
            results = read_sql(query, con, params=(body, target))
        return results

    def getAnnotationsWithTarget(target):
        target = target.replace('""', '"')
        with connect("annotations_metadata_2.db") as con:
            query = """SELECT id, body, target, motivation
            FROM Annotation
            WHERE target =?"""
        result = read_sql(query, con, params=(target,))
        return result

        
    
    
    def getEntitiesWithCreator(creator):
        creator = creator.replace('""', '"')
        with connect("annotations_metadata_2.db") as con:
            query = """ SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
                    FROM Creator
                    LEFT JOIN Collection ON Creator.internalID = Collection.internalID
                    LEFT JOIN Manifest ON Creator.internalID = Manifest.internalID
                    LEFT JOIN Canvas ON Creator.internalID = Canvas.internalID
                    WHERE creator=?
                    """
        result = read_sql(query, con, params=(creator,))
        return result
    
    def getEntitiesWithTitle(title):
        title = title.replace('""', '"')
        with connect("annotations_metadata_2.db") as con:
           
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

print(Relational_query_processor.getEntitiesWithCreator("Alighieri, Dante"))
# print(Relational_query_processor.getEntitiesWithTitle('Raimondi, Giuseppe. Quaderno manoscritto, ""Caserma Scalo : 1930-1968""'))
# print(Relational_query_processor.getAnnotationsWithBody("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"))
# print(Relational_query_processor.getAnnotationsWithTarget("https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1"))
# print(Relational_query_processor.getAnnotationsWithBodyAndTarget("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg", "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1")) 
# print(Relational_query_processor.getAllImages())
# print(Relational_query_processor.getAllAnnotations())


# FROM JournalArticle LEFT JOIN Journal ON 
#      JournalArticle.publicationVenue 
#      == 
#      Journal.internalId
# WHERE doi='10.1016/s1367-5931(02)00332-0'

# SELECT column1, column2
# FROM table1
# LEFT JOIN table2 ON table1.id = table2.id

# UNION

# SELECT column1, column2
# FROM table3
# LEFT JOIN table4 ON table3.id = table4.id;



#  query = """SELECT creator, title, Collection.internalID, Manifest.internalID, Canvas.internalID
#             FROM Creator
#             LEFT JOIN Collection ON Creator.internalID = Collection.internalID
#             LEFT JOIN Manifest ON Creator.internalID = Manifest.internalID
#             LEFT JOIN Canvas ON Creator.internalID = Canvas.manifestID
#             WHERE creator=?"""

# SELECT 
#     C.id AS collection_id,
#     C.internal_id AS collection_internal_id,
#     M.id AS manifest_id,
#     M.internal_id AS manifest_internal_id,
#     V.id AS canvas_id,
#     V.internal_id AS canvas_internal_id,
#     CI.creator AS creator,
#     CI.title AS title
# FROM 
#     Collection AS C
# LEFT JOIN 
#     Manifest AS M ON C.id = M.collection_id
# LEFT JOIN 
#     Canvas AS V ON M.id = V.manifest_id AND C.id = V.collection_id
# INNER JOIN 
#     Creator_item AS CI ON (CI.internal_id = C.internal_id OR CI.internal_id = M.internal_id)
# WHERE 
#     CI.creator = 'input-creator'

#  SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
#                     FROM Collection
#                     UNION 
#                     SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
#                     FROM Manifest
#                     WHERE title=?
#                     UNION
#                     SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
#                     FROM Canvas
#                     WHERE title=?
#                     UNION
#                     SELECT Creator.creator, Collection.id AS Collection_Id, Manifest.id AS Manifest_Id, Canvas.id AS Canvas_Id, Collection.title AS Collection_Title, Manifest.title AS Manifest_Title, Canvas.title AS Canvas_Title
#                     FROM Creator
#                     WHERE title=?
#                     """