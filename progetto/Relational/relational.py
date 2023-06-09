from pandas import read_csv, merge, Series, DataFrame, read_sql
from sqlite3 import connect
import pandas as pd
import re
def extract_id(s):               #aggiunto
    pattern = re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
    if pattern not in s:
        return None
    else:
        return pattern
annotations = read_csv("data/annotations.csv", keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})
#su annotations si può applicare extract_id e ottenere un internal id, dopodiché creare i dataframe necessari
# for idx, row in venues_ids.iterrows():
#     venue_internal_id.append("venue-" + str(idx))

#su annotations si può applicare extract_id (su /iiif/2/28429/annotation/p0001-image) e ottenere un internal id, dopodiché creare i dataframe necessari


# prova = DataFrame(annotations)
# print(prova)
# annotations_btm = annotations[["body", "target", "motivation"]]
# print(annotations_btm)
# body = annotations[["internalID", "body"]]
# target = annotations[["internalID", "target"]]
# motivation = annotations[["internalID", "motivation"]]

# df_joined_4 = merge(body, annotations_ids, on="internalID", how="left")
# df_joined_5 = merge(target, annotations_ids, on="internalID", how="left")
# df_joined_6 = merge(motivation, annotations_ids, on="internalID", how="left")


metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})
#metadata non dovrebbe avere sia "id" che "label"? (id ereditata e label sua) 
metadata.insert(0, "internalID", Series(metadata["id"].apply(extract_id), dtype="string"))


creator = metadata[["creator", "title", "internalID"]] 

# creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))  
#ma per creare "creator" non basterebbe prendere metadata selezionando solo id, internal id e creator e fare il rename su internalID? senza fare tutti questi procedimenti di metti e togli


collection = DataFrame()
manifest = DataFrame()
canvas = DataFrame()
collection_id = ""
manifest_id = ""
canvas_id = ""
for word, row in metadata.iterrows():
    if "collection" in row["id"]:
        collection_id = row["internalID"]
        collection = collection._append(row[["id", "internalID"]])
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
print(manifest)
print(df_joined_2)
# df_joined = merge(collection[["id", "internalID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_2 = merge(manifest[["id", "internalID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# df_joined_3 = merge(canvas[["id", "internalID", "manifestID", "collectionID"]], creator[["internalID", "title", "creator"]], on="internalID", how="left")
# abbiamo specificato how="left" per mantenere solo le corrispondenze tra gli internalID nei DataFrame di origine.
#i df_joined hanno sia la colonna internalId che EntityWithMetadataCreatorID che mostrano entrambe la stessa cosa
annotations.insert(0, "internalID", Series(annotations["id"].apply(extract_id), dtype="string"))
image = annotations[["body"]] 
image.insert(0, "imageID", Series(annotations["body"].apply(extract_id), dtype="string")) #ci deve stare body e un internal id fatto da body, in annotation body sarà l'internal id di questo
annotations_f=DataFrame()
try:
    annotations_j = merge(annotations, canvas, left_on="target", right_on="id", how="left")
    annotations_j = annotations_j[["internalID_x", "id_x", "body", "internalID_y", "motivation"]]
    annotations_j = annotations_j.rename(columns={"internalID_x":"internalID", "id_x":"id", "internalID_y":"target"})
    if annotations_j["target"].isna().any():
        pass
    else:
        annotations_f=annotations_j
except:
    print("Error")
try:
    annotations_j = merge(annotations, collection, left_on="target", right_on="id", how="left")
    annotations_j = annotations_j[["internalID_x", "id_x", "body", "internalID_y", "motivation"]]
    annotations_j = annotations_j.rename(columns={"internalID_x":"internalID", "id_x":"id", "internalID_y":"target"})
    if annotations_j[["target"]] == float("NaN"):
        pass
    else:
        annotations_f=annotations_j
except:
    print("No collection images found")
try:
    annotations_j = merge(annotations, manifest, left_on="target", right_on="id", how="left")
    annotations_j = annotations_j[["internalID_x", "id_x", "body", "internalID_y", "motivation"]]
    annotations_j = annotations_j.rename(columns={"internalID_x":"internalID", "id_x":"id", "internalID_y":"target"})
    if annotations_j[["target"]] == float("NaN"):
        pass
    else:
        annotations_f=annotations_j
except:
    print("No manifest images found")

print(annotations_f)

# collection = df_joined[["id", "internalID"]]
# # collection = collection.rename(columns={"EntityWithMetadataCreatorID": "internalId"})
# manifest = df_joined_2[["id", "internalID", "collectionID"]]
# # manifest = manifest.rename(columns={"EntityWithMetadataCreatorID": "internalId"})
# canvas = df_joined_3[["id", "internalID", "manifestID", "collectionID"]]
# # canvas = canvas.rename(columns={"EntityWithMetadataCreatorID": "internalId"})

print(collection)
print(manifest)
print(canvas)
print(creator)
# print(df_joined)
# print(df_joined_2)
# print(df_joined_3)


# with connect("annotations_metadata.db") as con:
#     creator.to_sql("Creator", con, if_exists="replace", index=False)
#     collection.to_sql("Collection", con, if_exists="replace", index=False)
#     manifest.to_sql("Manifest", con, if_exists="replace", index=False)
#     canvas.to_sql("Canvas", con, if_exists="replace", index=False)
#     con.commit()

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
    
# with connect("annotations_metadata.db") as con:
#     creator.to_sql("Creator", con, if_exists="replace", index=False)
#     collection.to_sql("Collection", con, if_exists="replace", index=False)
#     manifest.to_sql("Manifest", con, if_exists="replace", index=False)
#     canvas.to_sql("Canvas", con, if_exists="replace", index=False)
#     image.to_sql("Image", con, if_exists="replace", index=False)
#     annotations_f.to_sql("Annotation", con, if_exists="replace", index=False)
#     # df_joined.to_sql("DFJoined_1", con, if_exists="replace", index=False)
#     # df_joined_2.to_sql("DFJoined_2", con, if_exists="replace", index=False)
#     # df_joined_3.to_sql("DFJoined_3", con, if_exists="replace", index=False)
#     # df_joined_4.to_sql("DFJoined_4", con, if_exists="replace", index=False)
#     # df_joined_5.to_sql("DFJoined_5", con, if_exists="replace", index=False)
#     # df_joined_6.to_sql("DFJoined_6", con, if_exists="replace", index=False)
#     con.commit()