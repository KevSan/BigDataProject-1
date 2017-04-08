import pymongo
import pandas as pd
import xml.etree.ElementTree as ET

client = pymongo.MongoClient()
db = client.ROSMAP_RNASeq_entrez
ROSMAP_Collection = db.ROSMAP_Collection

df = pd.read_csv("./ROSMAP_RNASeq_entrez.csv")
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





tree = ET.parse("./uniprot-human.xml")
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

currDataFrame = pd.read_csv("./Entrez_Id_Uniprot.csv")
Ent_ID_Uni_Collection = db.Ent_ID_Uni_Collection
Ent_ID_Uni_Collection.insert_many(currDataFrame.to_dict('records'))
