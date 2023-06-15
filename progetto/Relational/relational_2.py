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
metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})

image = annotations[["body"]]
image.insert(0, "imageID", Series(annotations["body"].apply(extract_id), dtype="string"))

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
    if "canvas" in row["id"]:
        canvas = canvas._append(row[["id", "internalID"]]._append(Series({"manifestID":manifest_id, "collectionID":collection_id})), ignore_index=True)

df_joined = merge(collection, creator, on="internalID", how="left")
df_joined_2 = merge(manifest, creator, on="internalID", how="left")
df_joined_3 = merge(canvas, creator, on="internalID", how="left")

print(df_joined)
print(df_joined_2)
print(df_joined_3)

