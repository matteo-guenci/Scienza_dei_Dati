from rdflib import URIRef, Literal, RDF
from pandas import read_csv, Series
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