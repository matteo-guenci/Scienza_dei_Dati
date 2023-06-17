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
        
                
annotations = read_csv("data/annotations.csv", keep_default_na=False, dtype={"id":"string",
                                                                             "body":"string",
                                                                             "target":"string",
                                                                             "motivations":"string"})
metadata = read_csv("data/metadata.csv", keep_default_na=False, dtype={"id":"string",
                                                                       "title":"string",
                                                                       "creator":"string"})

image = annotations[["body"]]
image.insert(0, "imageID", Series(annotations["body"].apply(extract_id), dtype="string"))
image = image.rename(columns={"body":"image_url"})
    # annotations_j = annotations_j.rename(columns={"internalID_x":"internalID", "id_x":"id", "internalID_y":"target"})

# df_joined = merge(annotations, image, left_on="body", right_on="image_url")
# annotations = df_joined[["id", "imageID", "target", "motivation"]]
# annotations = annotations.rename(columns={"imageID":"body"})
# print(image)
print(annotations)
metadata.insert(0, "internalID", Series(metadata["id"].apply(extract_id), dtype="string"))


creator = metadata[["creator", "internalID"]] 

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
        collection = collection._append(row[["id", "internalID", "title"]])
    if "manifest" in row["id"]:
        manifest_id = row["internalID"]
        manifest = manifest._append(row[["id", "internalID", "title"]])
    if "canvas" in row["id"]:
        canvas = canvas._append(row[["id", "internalID", "title"]])
# df_joined = merge(collection, creator, on="internalID", how="left")
# df_joined_2 = merge(manifest, creator, on="internalID", how="left")
# df_joined_3 = merge(canvas, creator, on="internalID", how="left")
collection_items = DataFrame()
manifest_items = DataFrame()
# collection_items = merge(collection, manifest, left_on=None, right_on=None, left_index=False, right_index=False, how='inner', sort=False)
# for word, row in metadata.iterrows():
#     if "collection" in row["id"]:
#         collection_id = row["internalID"]
#         collection = collection.append(row[["id", "internalID"]])
#     if "manifest" in row["id"]:
#         manifest_id = row["internalID"]
#         manifest = manifest.append(row[["id", "internalID"]].append(Series({"collectionID":collection_id})), ignore_index=True)
#     if "canvas" in row["id"]:
#         canvas = canvas.append(row[["id", "internalID"]].append(Series({"manifestID":manifest_id, "collectionID":collection_id})), ignore_index=True)

# Create empty DataFrames to store the results
collection_items = pd.DataFrame()
manifest_items = pd.DataFrame()

# Iterate over rows in 'collection' DataFrame
for index, row in collection.iterrows():
    # Iterate over rows in 'metadata' DataFrame
    for index_2, row_2 in metadata.iterrows():
        if "manifest" in row_2["id"]:
            # print (row["id"], row_2["id"], is_part_of(row["internalID"], row_2["internalID"], "collection", "manifest"))
            if is_part_of(row["internalID"], row_2["internalID"], "collection", "manifest"):
                # Create a temporary DataFrame with the desired values
                temp_df = DataFrame({"collection_id": [row["internalID"]], "manifest_id": [row_2["internalID"]]})
                # Append the temporary DataFrame to 'collection_items'
                collection_items = collection_items._append(temp_df)
collection_items = collection_items.reset_index(drop=True)

# Iterate over rows in 'manifest' DataFrame
for index, row in manifest.iterrows():
    # Iterate over rows in 'metadata' DataFrame
    for index_2, row_2 in metadata.iterrows():
        if "canvas" in row_2["id"]:
            if is_part_of(row["internalID"], row_2["internalID"], "manifest", "canvas"):
                # Create a temporary DataFrame with the desired values
                temp_df = DataFrame({"manifest_id": [row["internalID"]], "canvas_id": [row_2["internalID"]]})
                # Append the temporary DataFrame to 'manifest_items'
                manifest_items = manifest_items._append(temp_df)
manifest_items = manifest_items.reset_index(drop=True)

# for word, row in collection.iterrows():
#     for word_2, row_2 in metadata.iterrows():
#         if "manifest" in row_2["id"]:
#             print(is_part_of(row["id"], row_2["id"], "collection", "manifest"))
#             if is_part_of(row["id"], row_2["id"], "collection", "manifest"):
#                 collection_items = collection_items._append(row["id"])
#                 collection_items = collection_items._append(row_2["id"])
#                 # collection_items= collection_items._append(row[["id"], row_2[["id"]]])
# for word, row in manifest.iterrows():
#     for word_2, row_2 in metadata.iterrows():
#         if "canvas" in row_2["id"]:
#             if is_part_of(row["id"], row_2["id"], "manifest", "canvas"):
#                 manifest_items = manifest_items._append(row["id"])
#                 manifest_items = manifest_items._append(row_2["id"])
#                 # manifest_items = manifest_items._append(row[["id"], row_2[["id"]]])


            
    
    
    # if "collection" in row["id"]:
    #     for word_2, row_2 in metadata.iterrows():
    #         if "manifest" in row_2["id"]:
    #             if is_part_of(row["id"], row_2["id"], "collection", "manifest"):
    #                 collection_items._append(row["id"])
            
    # if "manifest" in row["id"]:
    #     for word_2, row_2 in metadata.iterrows():
    #         if "canvas" in row_2["id"]:
    #             if is_part_of(row["id"], row_2["id"], "manifest", "canvas"):
    #                 manifest_items._append(row["id"])
# metadata.insert(0, "internalID", Series(metadata["id"].apply(extract_id), dtype="string"))
# print(collection)
# print(manifest)
# print(canvas)
# print(creator)
# print(collection_items)
# print(manifest_items)
# with connect("annotations_metadata_2.db") as con:
#     creator.to_sql("Creator", con, if_exists="replace", index=False)
#     collection.to_sql("Collection", con, if_exists="replace", index=False)
#     manifest.to_sql("Manifest", con, if_exists="replace", index=False)
#     canvas.to_sql("Canvas", con, if_exists="replace", index=False)
#     image.to_sql("Image", con, if_exists="replace", index=False)
#     annotations.to_sql("Annotation", con, if_exists="replace", index=False)
#     collection_items.to_sql("Collection_Items", con, if_exists="replace", index=False)
#     manifest_items.to_sql("Manifest_Items", con, if_exists="replace", index=False)

#     # df_joined.to_sql("DFJoined_1", con, if_exists="replace", index=False)
#     # df_joined_2.to_sql("DFJoined_2", con, if_exists="replace", index=False)
#     # df_joined_3.to_sql("DFJoined_3", con, if_exists="replace", index=False)
#     # df_joined_4.to_sql("DFJoined_4", con, if_exists="replace", index=False)
#     # df_joined_5.to_sql("DFJoined_5", con, if_exists="replace", index=False)
#     # df_joined_6.to_sql("DFJoined_6", con, if_exists="replace", index=False)
#     con.commit()