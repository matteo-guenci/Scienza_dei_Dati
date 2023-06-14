from pandas import read_csv, merge, Series, DataFrame, read_sql
from sqlite3 import connect
import pandas as pd
import re
def extract_id(s):               #aggiunto
    return re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
annotations = read_csv("data/annotations.csv", keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})
#su annotations si può applicare extract_id (su /iiif/2/28429/annotation/p0001-image) e ottenere un internal id, dopodiché creare i dataframe necessari
annotations.insert(0, "internalID", Series(annotations["id"].apply(extract_id), dtype="string"))
print(annotations)
annotations_ids = annotations[["internalID", "id"]]
print(annotations_ids)
# annotations_btm = annotations[["body", "target", "motivation"]]
# print(annotations_btm)
body = annotations[["internalID", "body"]]
target = annotations[["internalID", "target"]]
motivation = annotations[["internalID", "motivation"]]
# print(body)
# print(target)
# print(motivation)
df_joined_4 = merge(body, annotations_ids, on="internalID")
df_joined_5 = merge(target, annotations_ids, on="internalID")
df_joined_6 = merge(motivation, annotations_ids, on="internalID")
# body = df_joined_4
# target = df_joined_5
# motivation= df_joined_6

# print(df_joined_4)
# print(df_joined_5)
# print(df_joined_6)

# df_joined = merge(collection[["id", "internalID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_2 = merge(manifest[["id", "internalID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_3 = merge(canvas[["id", "internalID", "manifestID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})
#metadata non dovrebbe avere sia "id" che "label"? (id ereditata e label sua) 
metadata.insert(0, "internalID", Series(metadata["id"].apply(extract_id), dtype="string"))
# creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))


creator = metadata[["creator", "title", "internalID"]] 
# if ";" in creator:
#     creator = creator.split(";")
#     for i in creator:

# internal_ids=list(metadata["id"].apply(extract_id))   #aggiunto

    
# for i in range(len(internal_ids)):         #ho commentato la sezione perché nel modo qui di sopra si ottiene lo stesso risultato più velocemente
#     internal_ids[i]=re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",internal_ids[i]).group()
    #ids[i]=re.search(r"iiif\/(.+)$",ids[i])
    #nuoviids[i]=re.search(r"iiif\/(.+)$",ids[i])
#print (ids)



# metadata_internal_id = []
# for idx, row in creator.iterrows():
#     metadata_internal_id.append(str(idx))

# creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))  
#ma per creare "creator" non basterebbe prendere metadata selezionando solo id, internal id e creator e fare il rename su internalID? senza fare tutti questi procedimenti di metti e togli

# venues_ids.insert(0, "venueId", Series(venue_internal_id, dtype="string"))

# print(metadata_internal_id)
#print(creator)
#word_to_find = "collection"
# collection = metadata.query("id.str.contains(@word_to_find, case=False)")
#print(collection)
collection = DataFrame()
manifest = DataFrame()
canvas = DataFrame()
collection_id = ""
manifest_id = ""
canvas_id = ""
for word, row in metadata.iterrows():      #modificato
    if "collection" in row["id"]:
        collection_id = row["internalID"]
        collection = collection._append(row[["id", "internalID"]])   #filtered_df = pd.concat([filtered_df, row], axis=1)
    if "manifest" in row["id"]:
        manifest_id = row["internalID"]
        manifest = manifest._append(row[["id", "internalID"]]._append(Series({"collectionID":collection_id})), ignore_index=True)
        # manifest = manifest._append(row._append(Series({"collectionID":collection_id})), ignore_index=True)
    if "canvas" in row["id"]:
        canvas = canvas._append(row[["id", "internalID"]]._append(Series({"manifestID":manifest_id, "collectionID":collection_id})), ignore_index=True)
#o appendi colonne specifiche nell'iterazione qui di sopra oppure selezioni colonne specifiche nella merge qui di sotto per ottenere il data frame che vuoi
#ora tocca fare le merge
df_joined = merge(collection, creator, on="internalID", how="left")
df_joined_2 = merge(manifest, creator, on="internalID", how="left")
df_joined_3 = merge(canvas, creator, on="internalID", how="left")
# df_joined = merge(collection[["id", "internalID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_2 = merge(manifest[["id", "internalID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_3 = merge(canvas[["id", "internalID", "manifestID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# abbiamo specificato how="left" per mantenere solo le corrispondenze tra gli internalID nei DataFrame di origine.
#i df_joined hanno sia la colonna internalId che EntityWithMetadataCreatorID che mostrano entrambe la stessa cosa
print(collection)
print(manifest)
print(canvas)
print(creator)
# collection = df_joined[["id", "internalID"]]
# # collection = collection.rename(columns={"EntityWithMetadataCreatorID": "internalId"})
# manifest = df_joined_2[["id", "internalID", "collectionID"]]
# # manifest = manifest.rename(columns={"EntityWithMetadataCreatorID": "internalId"})
# canvas = df_joined_3[["id", "internalID", "manifestID", "collectionID"]]
# # canvas = canvas.rename(columns={"EntityWithMetadataCreatorID": "internalId"})

# print(collection)
# print(manifest)
# print(canvas)
# print(creator)
print(df_joined)
print(df_joined_2)
print(df_joined_3)


with connect("annotations_metadata.db") as con:
    creator.to_sql("Creator", con, if_exists="replace", index=False)
    collection.to_sql("Collection", con, if_exists="replace", index=False)
    manifest.to_sql("Manifest", con, if_exists="replace", index=False)
    canvas.to_sql("Canvas", con, if_exists="replace", index=False)
    annotations_ids.to_sql("Annotations_ids", con, if_exists="replace", index=False)
    body.to_sql("Body", con, if_exists="replace", index=False)
    target.to_sql("Target", con, if_exists="replace", index=False)
    motivation.to_sql("Motivation", con, if_exists="replace", index=False)
    con.commit()

# with connect("annotations_metadata.db") as con:
#     query = "SELECT title FROM Collection"
#     query_2 = "SELECT * FROM Manifest"
#     query_3 = """SELECT id
#                 FROM Manifest
#                 WHERE internalID='2/28429/canvas/p1'
#                 UNION
#                 SELECT id
#                 FROM Canvas
#                 WHERE internalID='2/28429/canvas/p1'"""
#     df_sql = read_sql(query, con)
#     df_sql_2 = read_sql(query_2, con)
#     df_sql_3 = read_sql(query_3, con)
    
# print(df_sql)
# print(df_sql_2["collectionID"])
# print(df_sql_3)