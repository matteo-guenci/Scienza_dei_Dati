class IdentifiableEntity(object):
     def __init__(self, id):
         self.id = id

class Annotation(IdentifiableEntity):
    def __init__(self, motivation, target, body):
        self.motivation = motivation
        self.target = target
        self.body = body

    def getTarget(self):
        return self.target
    
class Image(IdentifiableEntity):
    pass


class EntityWithMetadataCreators(IdentifiableEntity):
    def __init__(self, creator):
        self.creator = creator
        
    

        # self.creators = set()
        # for creator in creators:
        #     self.creators.add(creator)

class Collection(EntityWithMetadataCreators):
    def __init__(self, label, title):
        # self.items = set()
        self.label = label
        self.title = title
        self.items = list()       
        # for item in items:
        #     self.items.add(item)

class Manifest(EntityWithMetadataCreators):
    def __init__(self, label, title):
        self.label = label
        self.title = title 
        self.items = list()
        # self.items = set()
        # for item in items:
        #   self.items.add(item)

class Canvas(EntityWithMetadataCreators):
    def __init__(self, label, title):
        self.label = label
        self.title = title
        
class CollectionItems(Manifest):
    def __init__(self, item_id, item):
        self.item_id = item_id
        self.item = item
        
class ManifestItems(Canvas):
    def __init__(self, item_id, item):
        self.item_id = item_id
        self.item = item



# annotation_1 = Annotation('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0001-image','"https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"','https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1,painting')
# annotation_2 = Annotation('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0002-image','"https://dl.ficlit.unibo.it/iiif/2/45499/full/699,800/0/default.jpg"','https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p2,painting')

# print("The objects in 'annotation_1' and 'annotation_2' share the same class -->", type(annotation_1) == type(annotation_2))
# print("Indeed, the types of the two objects are both", type(annotation_1))

# print("\nThe objects in 'annotation_1' and 'annotation_2' are the same object -->", id(annotation_1) == id(annotation_2))
# print("Indeed, the integers identifying the two objects are", id(annotation_1), "and", id(annotation_2), "respectively")