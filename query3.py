from pprint import pprint
import pymongo
import pandas as pd
import xml.etree.ElementTree as ET

class QueryThree:

    def xmlMongoImport(self, stringInput):
        try:
            file = open(stringInput)
        except IOError:
            print("That file doesn't seem to exist")
        else:
            tree = ET.parse(stringInput)
            root = tree.getroot()
            accessionList = []
            entryNameList = []
            proteinList = []
            geneList = []
            organismList = []
            referenceList = []
            commentsList = []
            proteinExistenceList = []
            keyWordsList = []
            featureList = []
            evidenceList = []
            sequenceList = []
            for entry in root.findall('{http://uniprot.org/uniprot}entry'):

                #storess accession numbers in list
                entryAcc = entry.findall('{http://uniprot.org/uniprot}accession')
                currAccList = []
                for acc in entryAcc:
                    currAccList.append(acc.text)
                accessionList.append(currAccList)

                #stores entry names in list
                entryName = entry.find('{http://uniprot.org/uniprot}name').text
                entryNameList.append(entryName)

                #stores proteins in list
                entryProtein = entry.findall('{http://uniprot.org/uniprot}protein')
                currProteinList = []
                for protein in entryProtein:
                    for child in protein:
                        for grandChild in child:
                            currDict = {child.tag[28:] : grandChild.text}
                            currProteinList.append(currDict)
                proteinList.append(currProteinList)

                #stores genes in list
                genes = entry.findall('{http://uniprot.org/uniprot}gene')
                currGeneList = []
                for gene in genes:
                    for child in gene:
                        currDict = {child.tag[28:] : child.text}
                        currGeneList.append(currDict)
                geneList.append(currGeneList)

                #stores organisms in list
                organisms = entry.findall('{http://uniprot.org/uniprot}organism')
                currOrganismList = []
                for organism in organisms:
                    for child in organism:
                        if child.tag[28:] == "lineage":
                            currLinList = []
                            for grandChild in child:
                                currLinList.append(grandChild.text)
                                currLinDict = {child.tag[28:] : currLinList}
                                currOrganismList.append(currLinDict)
                            else:
                                currDict = {child.tag[28:], child.text}
                                currOrganismList.append(currDict)
                organismList.append(currOrganismList[0])

                #stores references in list
                references = entry.findall('{http://uniprot.org/uniprot}reference')
                currRefList = []
                for ref in references:
                    for cit in ref:
                        for child in cit:
                            if child.tag != '{http://uniprot.org/uniprot}authorList' and child.tag != '{http://uniprot.org/uniprot}dbReference':
                                currDict = {child.tag[28:] : child.text}
                                currRefList.append(currDict)
                referenceList.append(currRefList)

                #stores comments in a list
                comments = entry.findall('{http://uniprot.org/uniprot}comment')
                currComList = []
                for comment in comments:
                    for child in comment:
                        currDict = {child.tag[28:] : child.text}
                        currComList.append(currDict)
                commentsList.append(currComList)

                #stores protein existences in list
                proteinExistences = entry.findall('{http://uniprot.org/uniprot}proteinExistence')
                currProExistList = []
                for pro in proteinExistences:
                    innerDict = {"None" : pro.attrib}
                    outerDict = {pro.tag[28:] : innerDict}
                    currProExistList.append(outerDict)
                proteinExistenceList.append(currProExistList)

                #stores keywords in list
                keywords = entry.findall('{http://uniprot.org/uniprot}keyword')
                currKeyWordsList = []
                for key in keywords:
                    currDict = {key.tag[28:] : key.text}
                    currKeyWordsList.append(currDict)
                keyWordsList.append(currKeyWordsList)

                #stores features in list
                features = entry.findall('{http://uniprot.org/uniprot}feature')
                originalList = []
                variationList = []
                currFetList = []
                for fet in features:
                    original = fet.find('{http://uniprot.org/uniprot}original')
                    if original != None:
                        if original.text != '':
                            originalList.append(original.text)
                    variation = fet.find('{http://uniprot.org/uniprot}variation')
                    if variation != None:
                        if variation.text != '':
                            variationList.append(variation.text)

                    original = {"original" : originalList}
                    variation = {"variation": variationList}
                    fetList = []
                    fetList.append(original)
                    fetList.append(variation)
                    currFetList.append(fetList)
                featureList.append(currFetList[0])

                #stores evidence in list
                currEvidenceList = []
                evidences = entry.findall('{http://uniprot.org/uniprot}evidence')
                for evidence in evidences:
                    currEvidenceList.append(evidence.attrib)
                evidenceList.append(currEvidenceList)


                #stores sequences in list
                currSequenceList = []
                sequences = entry.findall('{http://uniprot.org/uniprot}sequence')
                for seq in sequences:
                    currText = seq.text
                    currText = currText.replace('\n', '')
                    currSequenceList.append(currText)
                sequenceList.append(currSequenceList)

            print("Parsing XML file has been completed")
            data = {"accession" : accessionList,
                    "uniprot_id" : entryNameList,
                    "protein" : proteinList,
                    "gene" : geneList,
                    "organism" : organismList,
                    "reference" : referenceList,
                    "comments" : commentsList,
                    "proteinExistence" : proteinExistenceList,
                    "keywords" : keyWordsList,
                    "feature" : featureList,
                    "evidence" : evidenceList,
                    "sequence" : sequenceList
            }
            print("Please be patient while the data is uploaded into Mongo")
            print("This can take up to 10 minutes")
            client = pymongo.MongoClient()

            frame = pd.DataFrame(data)
            db = client.Entrez_Id_Uniprot
            uniprot_human_collections = db.uniprot_human_collections
            uniprot_human_collections.insert_many(frame.to_dict('records'))

    def csvMongoImport(self, strInput):
        client = pymongo.MongoClient()
        db = client.Entrez_Id_Uniprot

        try:
            file = open(strInput)
        except IOError:
                print("That file doesn't seem to exist")
        else:
            currDataFrame = pd.read_csv(strInput)
            Ent_ID_Uni_Collection = db.Ent_ID_Uni_Collection
            Ent_ID_Uni_Collection.insert_many(currDataFrame.to_dict('records'))

    #this function works, but it is too slow
    def searchViaUniprotIDJoin(self, uniprotID):
        client = pymongo.MongoClient()
        pipeline = [
            {"$lookup":{
                "from" : "Ent_ID_Uni_Collection",
                "localField" : "uniprot_id",
                "foreignField" : "uniprot_id",
                "as" : "queryResult"
                }

            }, {
            "$match" : {
                "queryResult.uniprot_id" : uniprotID
                }
            }
        ]
        db = client.Entrez_Id_Uniprot
        uniprot_human_collections = db.uniprot_human_collections
        pprint(list(uniprot_human_collections.aggregate(pipeline)))

    def findViaUniprotID(self, uniprotID):
        client = pymongo.MongoClient()
        db = client.Entrez_Id_Uniprot
        uniprot_human_collections = db.uniprot_human_collections
        Ent_ID_Uni_Collection = db.Ent_ID_Uni_Collection
        pprint(list(uniprot_human_collections.find({"uniprot_id" : uniprotID})))
        entList = list(Ent_ID_Uni_Collection.find({"uniprot_id" : uniprotID}))
        if len(entList) > 0:
            print("Gene Name : ",entList[0]['Gene Name'])
            print("entrez_id : ",entList[0]['entrez_id'])

    def findViaEntrezID(self, entrezID):
        client = pymongo.MongoClient()
        db = client.Entrez_Id_Uniprot
        uniprot_human_collections = db.uniprot_human_collections
        Ent_ID_Uni_Collection = db.Ent_ID_Uni_Collection
        try:
            number = int(entrezID)
        except ValueError:
            print("I am afraid %s is not a number" % entrezID)
        else:
            entList = list(Ent_ID_Uni_Collection.find({"entrez_id" : int(entrezID)}))
            entLen = len(entList)
            for x in range(0, entLen):
                if entLen > 0:
                    uniprotID = entList[x]['uniprot_id']
                    pprint(list(uniprot_human_collections.find({"uniprot_id" : uniprotID})))

                    print("Gene Name : ",entList[x]['Gene Name'])
                    print("uniprot_id : ",entList[x]['uniprot_id'])
                    print("entrez_id : ",entList[x]['entrez_id'], "\n")

    def main_method(self):
        val = int(input("Press 1 if you wish to load a new xml file\nPress 2 if you wish to load a new Entrez/Uniprot csv file\nPress 3 if you wish to query the database: "))
        if val == 1:
            print("___________________________")
            xmlInput = input("Please enter the xml file path and name: ")
            self.xmlMongoImport(xmlInput)
        if val == 2:
            print("___________________________")
            csvInput = input("Please enter the csv file path and name: ")
            self.csvMongoImport(csvInput)
        if val == 3:
            print("___________________________")
            val2 = int(input("Press 1 if you wish to query by entrez_id\nPress 2 if you wish to query by uniprot_id: "))
            if val2 == 1:
                print("___________________________")
                strInput1 = input("Please enter a entrez ID(must be an integer): ")
                self.findViaEntrezID(strInput1)
            if val2 == 2:
                print("___________________________")
                strInput = input("Please enter a uniprot ID: ")
                self.findViaUniprotID(strInput)




    #"./uniprot-human.xml"
