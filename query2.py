from pprint import pprint
import pymongo
import pandas as pd

class QueryTwo:

    def insertFile(self, fileInput):
        client = pymongo.MongoClient()
        db = client.ROSMAP_RNASeq_entrez
        ROSMAP_Collection = db.ROSMAP_Collection

        try:
            file = open(fileInput)
        except IOError:
            print("That file doesn't seem to exist")
        else:
            df = pd.read_csv(fileInput)
            rowCount = len(df['DIAGNOSIS'])

            df.insert(0, 'Diseases', "")

            for x in range(0, rowCount):
                currValue = df['DIAGNOSIS'][x]
                if currValue != 'NaN':
                    if currValue == 1:
                        df.set_value(x,'Diseases','NCI')
                    elif currValue == 2 or currValue == 3:
                        df.set_value(x,'Diseases','MCI')
                    elif currValue == 4 or currValue == 5:
                        df.set_value(x,'Diseases','AD')
                    elif currValue == 6:
                        df.set_value(x,'Diseases','Others')

            df = df.drop("DIAGNOSIS", axis=1)
            df = df.drop("PATIENT_ID", axis=1)
            ROSMAP_Collection.insert_many(df.to_dict('records'))


    def geneQueryEntrez(self, geneInput):
        client = pymongo.MongoClient()
        db = client.ROSMAP_RNASeq_entrez

        ROSMAP_Collection = db.ROSMAP_Collection

        strDolSign = "$"
        pipeline = [
            {"$group": {
                "_id" : "$Diseases",
                "Avg" : {"$avg" : strDolSign+geneInput},
                "Std" : {"$stdDevPop" : strDolSign+geneInput}
                }
            }
        ]
        pprint(list(db.ROSMAP_Collection.aggregate(pipeline)))

    def geneQueryUniprot(self, uniprotInput):
        client = pymongo.MongoClient()
        db = client.ROSMAP_RNASeq_entrez
        db1 = client.Entrez_Id_Uniprot
        Ent_ID_Uni_Collection = db1.Ent_ID_Uni_Collection
        uniprotList = list(Ent_ID_Uni_Collection.find({"uniprot_id" : uniprotInput}))
        if len(uniprotList) > 0:
            geneInput = str(uniprotList[0]['entrez_id'])

            ROSMAP_Collection = db.ROSMAP_Collection

            strDolSign = "$"
            pipeline = [
                {"$group": {
                    "_id" : "$Diseases",
                    "Avg" : {"$avg" : strDolSign+geneInput},
                    "Std" : {"$stdDevPop" : strDolSign+geneInput}
                    }
                }
            ]
            pprint(list(db.ROSMAP_Collection.aggregate(pipeline)))
        else:
            print("That uniprot_id isn't in the database")

    def main_method(self):
        val = int(input("Press 1 if you wish to load a new file\nPress 2 if you wish to get the avg and std of a given gene: "))
        if val == 1:
            print("___________________________")
            file_name = input("Please enter the file path and name: ")
            self.insertFile(file_name)
        if val == 2:
            print("___________________________")
            val1 = int(input("Press 1 if you wish to search by uniprot_id\nPress 2 if you wish to search via entrez_id: "))
            if val1 == 1:
                print("___________________________")
                uniprot_id = input("Please enter the uniprot_id: ")
                self.geneQueryUniprot(uniprot_id)
            if val1 == 2:
                print("___________________________")
                entrez_id = input("Please enter the entrez_id: ")
                self.geneQueryEntrez(entrez_id)
