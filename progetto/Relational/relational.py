from pandas import read_csv, merge, Series, DataFrame
from sqlite3 import connect

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
metadata_internal_id = []
for idx, row in creator.iterrows():
    metadata_internal_id.append(str(idx))

creator.insert(0, "", Series(metadata_internal_id, dtype="string"))
    
# venues_ids.insert(0, "venueId", Series(venue_internal_id, dtype="string"))

print(metadata_internal_id)
print(creator)


# venues = read_csv("../01/01-venues.csv", 
#                   keep_default_na=False,
#                   dtype={
#                       "id": "string",
#                       "name": "string",
#                       "type": "string"
#                   })

# # This will create a new data frame starting from 'venues' one,
# # and it will include only the column "id"
# venues_ids = venues[["id"]]

# # Generate a list of internal identifiers for the venues
# venue_internal_id = []
# for idx, row in venues_ids.iterrows():
#     venue_internal_id.append("venue-" + str(idx))

# # Add the list of venues internal identifiers as a new column
# # of the data frame via the class 'Series'
# venues_ids.insert(0, "venueId", Series(venue_internal_id, dtype="string"))

# # Show the new data frame on screen
# venues_ids