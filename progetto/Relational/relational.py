from pandas import read_csv, merge, Series, DataFrame
from sqlite3 import connect
import pandas as pd
import re
def extract_id(s):               #aggiunto
    return re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",s).group()
annotations = read_csv("data/annotations.csv", keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})

# for idx, row in venues_ids.iterrows():
#     venue_internal_id.append("venue-" + str(idx))


metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})
metadata.insert(0, "internalID", Series(metadata["id"].apply(extract_id), dtype="string"))
# creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))


creator = metadata[["creator", "id"]] 
# if ";" in creator:
#     creator = creator.split(";")
#     for i in creator:

internal_ids=list(metadata["id"].apply(extract_id))   #aggiunto

    
# for i in range(len(internal_ids)):         #ho commentato la sezione perché nel modo qui di sopra si ottiene lo stesso risultato più velocemente
#     internal_ids[i]=re.search("(?<=iiif\/)[0-9_a-zA-Z](.+)",internal_ids[i]).group()
    #ids[i]=re.search(r"iiif\/(.+)$",ids[i])
    #nuoviids[i]=re.search(r"iiif\/(.+)$",ids[i])
#print (ids)



metadata_internal_id = []
for idx, row in creator.iterrows():
    metadata_internal_id.append(str(idx))

creator.insert(0, "EntityWithMetadataCreatorID", Series(internal_ids, dtype="string"))

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
        collection = collection.append(row)   #filtered_df = pd.concat([filtered_df, row], axis=1)
    if "manifest" in row["id"]:
        manifest_id = row["internalID"]
        manifest = manifest.append(row.append(Series({"collectionID":collection_id})), ignore_index=True)
    if "canvas" in row["id"]:
        canvas = canvas.append(row.append(Series({"manifestID":manifest_id, "collectionID":collection_id})), ignore_index=True)
print(collection)
print(manifest)
print(canvas)

#ora tocca fare le merge
df_joined = merge(collection, creator, left_on="id", right_on="id") 
df_joined_2 = merge(manifest, creator, left_on="id", right_on="id") 
df_joined_3 = merge(canvas, creator, left_on="id", right_on="id") 

#print(df_joined_2)
collection = df_joined[["id", "title", "EntityWithMetadataCreatorID"]]
collection = collection.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
manifest = df_joined_2[["id", "title", "EntityWithMetadataCreatorID"]]
manifest = manifest.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
canvas = df_joined_3[["id", "title", "EntityWithMetadataCreatorID"]]
canvas = canvas.rename(columns={"EntityWithMetadataCreatorID" : "internalId"})
# print(collection)
# print(manifest)
# print(canvas)
# print(creator)
#print(metadata)