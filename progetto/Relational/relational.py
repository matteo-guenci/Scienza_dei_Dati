from pandas import read_csv, merge, Series, DataFrame
from sqlite3 import connect
import pandas as pd
import re

annotations = read_csv("data/annotations.csv", keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})

# for idx, row in venues_ids.iterrows():
#     venue_internal_id.append("venue-" + str(idx))


metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})
creator = metadata[["creator"]]
# if ";" in creator:
#     creator = creator.split(";")
#     for i in creator:

internal_ids=list(metadata ["id"])  


for i in range(len(internal_ids)):
    internal_ids[i]=re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",internal_ids[i]).group()
    #ids[i]=re.search(r"iiif\/(.+)$",ids[i])
    #nuoviids[i]=re.search(r"iiif\/(.+)$",ids[i])
#print (ids)



metadata_internal_id = []
for idx, row in creator.iterrows():
    metadata_internal_id.append(str(idx))

creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))
    
# venues_ids.insert(0, "venueId", Series(venue_internal_id, dtype="string"))

# print(metadata_internal_id)
print(creator)
word_to_find = "collection"
# collection = metadata.query("id.str.contains(@word_to_find, case=False)")
#print(collection)

collection = pd.DataFrame()
manifest = pd.DataFrame()
canvas = pd.DataFrame()
for word , row in metadata.iterrows():
    if "collection" in row["id"]:
        collection = collection.append(row)   #filtered_df = pd.concat([filtered_df, row], axis=1)
    if "manifest" in row["id"]:
        manifest = manifest.append(row)
    if "canvas" in row["id"]:
        canvas = canvas.append(row)        
# print(collection)
# print(manifest)
# print(canvas)

#ora tocca fare le merge
df_joined = merge(collection, creator, left_on="creator", right_on="creator") 
df_joined_2 = merge(manifest, creator, left_on="creator", right_on="creator") 
df_joined_3 = merge(canvas, creator, left_on="creator", right_on="creator") 

# print(df_joined)
collection = df_joined[["id", "title", "EntityWithMetadataCreatorID"]]
collection = collection.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
manifest = df_joined_2[["id", "title", "EntityWithMetadataCreatorID"]]
manifest = manifest.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
canvas = df_joined_3[["id", "title", "EntityWithMetadataCreatorID"]]
canvas = canvas.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
print(collection)
print(manifest)
print(canvas)