import pymongo

client = pymongo.MongoClient()
client.drop_database("Entrez_Id_Uniprot")
client.drop_database("ROSMAP_RNASeq_entrez")
print("Databases have been dropped")
