class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

class Annotation(IdentifiableEntity):
    def __init__(self, motivation, target, body):
        self.motivation = motivation
        self.target = target
        self.body = body

class Image(IdentifiableEntity):
    pass


class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, label, title, creators):
        self.label = label
        self.title = title
        self.creators = set()
        for creator in creators:
            self.id.add(creator)

class Collection(EntityWithMetadata):
    def __init__(self, items):
        self.item = set()
        for item in items:
            self.item.add(item)

class Manifest(EntityWithMetadata):
    def __init__(self, items):
        self.item = set()
        for item in items:
          self.item.add(item)

class Canvas(EntityWithMetadata):
    pass

annotation_1 = Annotation('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0001-image','"https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"','https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1,painting')
annotation_2 = Annotation('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0002-image','"https://dl.ficlit.unibo.it/iiif/2/45499/full/699,800/0/default.jpg"','https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p2,painting')

print("The objects in 'annotation_1' and 'annotation_2' share the same class -->", type(annotation_1) == type(annotation_2))
print("Indeed, the types of the two objects are both", type(annotation_1))

print("\nThe objects in 'annotation_1' and 'annotation_2' are the same object -->", id(annotation_1) == id(annotation_2))
print("Indeed, the integers identifying the two objects are", id(annotation_1), "and", id(annotation_2), "respectively")