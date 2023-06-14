from rdflib import URIRef, Literal, RDF
from pandas import read_csv, Series
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from rdflib import *
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

#impostazione della creazione di URI: 
#primo metodo == utilizzare risorse esistenti ad esempio su schema.org
#Image = URIRef("https://schema.org/ImageObject")
#Collection = URIRef("https://schema.org/Collection")
#secondo metodo == create your own. In the latter case, you have to remind to use an URL you are in control of 
# (e.g. your website or GitHub repository). For instance, a possible pattern for defining your own name 
# for the class `Book` could be `https://<your website>/Book` (e.g. `https://essepuntato.it/Book`).

#Collection = URIRef("https://github.com/matteo-guenci/Scienza_dei_Dati/Collection") #(?)
#Manifest = URIRef("https://github.com/matteo-guenci/Scienza_dei_Dati/Manifest")
#Canvas = URIRef("https://github.com/matteo-guenci/Scienza_dei_Dati/Canvas")



#Esempio lezioni handson
# # classes of resources
# JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
# BookChapter = URIRef("https://schema.org/Chapter")
# Journal = URIRef("https://schema.org/Periodical")
# Book = URIRef("https://schema.org/Book")

# # attributes related to classes
# doi = URIRef("https://schema.org/identifier")
# publicationYear = URIRef("https://schema.org/datePublished")
# title = URIRef("https://schema.org/name")
# issue = URIRef("https://schema.org/issueNumber")
# volume = URIRef("https://schema.org/volumeNumber")
# identifier = URIRef("https://schema.org/identifier")
# name = URIRef("https://schema.org/name")

# # relations among classes
# publicationVenue = URIRef("https://schema.org/isPartOf")

with open("./data/collection-1.json", "r", encoding="utf-8") as f:
    json_doc = load(f)

iiif_prezi = Namespace("http://iiif.io/api/presentation/3#")
ast = Namespace("http://www.w3.org/ns/activitystreams#")
rdf= Namespace("http://www.w3.org/2000/01/rdf-schema#")

k_graph= Graph()

k_graph.bind("iiif_prezi", iiif_prezi)
k_graph.bind("ast", ast)
k_graph.bind("rdf", rdf)

Item = URIRef (ast+"items")
Label=URIRef (rdf+"label")

def level (father, json_doc):
    for i in json_doc:
        if i == "id":
            
            id=URIRef(json_doc[i])
            k_graph.add((father, Item, id))
            
        if i == "type":
            k_graph.add((id, RDF.type, URIRef(iiif_prezi+json_doc[i])))
        if i == "label":
            for j in json_doc[i]:
                for k in json_doc[i][j]:
                    k_graph.add((id, Label, Literal(k)))
        if i == "items":
            for j in json_doc[i]:
                level (id, j)
        else: pass
    

for i in json_doc:
    if i == "id":
        id=URIRef(json_doc[i])
    if i == "type":
        k_graph.add((id, RDF.type, URIRef(iiif_prezi+json_doc[i])))
    if i == "label":
        for j in json_doc[i]:
            for k in json_doc[i][j]:
                k_graph.add((id, Label, Literal(k)))
    if i == "items":
        for j in json_doc[i]:
            level (id, j)
    else: pass

    

store = SPARQLUpdateStore()

# The URL of the SPARQL endpoint is the same URL of the Blazegraph
# instance + '/sparql'
endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))

#delete_query="""DELETE WHERE {?s ?p ?o.}"""

for triple in k_graph.triples((None, None, None)):
    store.add(triple)
    
#store.update(delete_query)
# Once finished, remeber to close the connection
store.close()