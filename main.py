from multiprocessing.sharedctypes import Value
import mysql.connector
import requests
import sys
import xml.etree.ElementTree as ET
from requests_html import HTMLSession
import pandas as pd
from bs4 import BeautifulSoup
import time

def GetIDNext(ID):
    return (ID+1)

def GetNewID(ID,StartID):
    if(ID[0][0]==None):
        nextID=StartID
    else:
        nextID=GetIDNext(ID[0][0])
    return nextID

def getDB():
    DB=mysql.connector.connect(host="localhost",user="root",database="summer_project")
    return DB

def DBSelect(SQL):
    mydb =getDB()
    mycursor = mydb.cursor()
    mycursor.execute(SQL)
    myresult = mycursor.fetchall()
    if mydb and mycursor:                        
        mydb.close() 
        mycursor.close() 
    return myresult

def DBSelectWithValues(SQL,val):
    mydb =getDB()
    mycursor = mydb.cursor()
    mycursor.execute(SQL,val)
    myresult = mycursor.fetchall()
    if mydb and mycursor:                        
        mydb.close() 
        mycursor.close() 
    return myresult

def DBInsert(SQL,Val):
    mydb = getDB()
    mycursor = mydb.cursor()
    mycursor.execute(SQL,Val)
    mydb.commit()
    myresult = mycursor.fetchall()
    if mydb and mycursor:                        
        mydb.close() 
        mycursor.close() 
    return myresult

def GetIRITTeamNames(url):
    s = HTMLSession()
    r = s.get(url)
    team = r.html.find('#tab-3 ul li a')
    members=[]
    for member in team:
        members.append(member.text)
    return members

def SubmitIRITDepartment(sortname,fullname,link,country):
    DepartmentName=sortname
    DescriptiveName=fullname
    website=link
    Country=country
    StartID=1000000
    ID=DBSelect("SELECT MAX(RGroupID) FROM researchgroup")
    nextID=GetNewID(ID,StartID)
    Sql="INSERT INTO `researchgroup` (`RGroupID`, `RGroupName`, `RGroupFullName`, `RGroupCountry`) VALUES (%s, %s, %s,%s);"
    Val=(nextID,DepartmentName,DescriptiveName,Country)
    try:
        DBInsert(Sql,Val)
        SubmitIRITTeam(link,sortname)
    except:
        print("SubmitIRITDepartment Data Insertion Failed")

def getReaserchTeamID(departmentname):
    val=(departmentname,)
    Sql="SELECT RGroupID FROM `researchgroup` WHERE RGroupName=%s"
    ID=DBSelectWithValues(Sql,val)
    return ID

def isAuthorunique(AuthorFname,AuthorLname):
    Sql="SELECT AuthorID FROM `author` WHERE FirstName=%s and Surname=%s"
    val=(AuthorFname,AuthorLname)
    AuthorID=DBSelectWithValues(Sql,val)
    if(len(AuthorID)==0):
        return True
    else:
        return False

def SubmitIRITTeam(url,departmentname):
    TeamMembers=[]
    Team=GetIRITTeamNames(url)
    for Member in Team:
        TeamMembers.append(Member)
    GroupID=getReaserchTeamID(departmentname)
    for Author in TeamMembers:
        FullName=str(Author).split()
        if(FullName[0] and FullName[1]):
            AuthorFname=FullName[0]
            AuthorLname=FullName[1]
            if(isAuthorunique(AuthorFname,AuthorLname)==True):
                StartID=2000000
                ID=DBSelect("SELECT Max(AuthorID) FROM author")
                nextID=GetNewID(ID,StartID)
                Sql="INSERT INTO `author` (`AuthorID`, `FirstName`, `Surname`) VALUES (%s, %s, %s);"
                val=(nextID,AuthorFname,AuthorLname)
                try:
                    DBInsert(Sql,val)
                except:
                    print("SubmitIRITTeam Author Data Insertion Failed")
                Sql="INSERT INTO `authorrgroup` (`AuthorID`, `RGroupID`) VALUES (%s, %s);"
                val=(nextID,GroupID[0][0])
                try:
                    DBInsert(Sql,val)
                except:
                    print("SubmitIRITTeam AuthorGroup Data Insertion Failed")

def GetAuthors(GroupName):
    Sql="SELECT A.AuthorID,A.FirstName,A.SurName FROM Author as A,authorrgroup as AG,researchgroup as RG where A.AuthorID=AG.AuthorID and AG.RGroupID=RG.RGroupID and RgroupName=%s"
    val=(GroupName,)
    Authors=DBSelectWithValues(Sql,val)
    return Authors 

def CompareAuthorPublicationsWithDb(PubName,PaperType,Year):
    Sql="SELECT PubID FROM `publication` WHERE PubName=%s and PubType=%s and PubYear=%s"
    val=(PubName,PaperType,Year)
    PubID=DBSelectWithValues(Sql,val)
    if(len(PubID)==0):
        return True
    else:
        return False

def CleanSubjectArea(research_topic):
    ToRemove=["Field Of Research:","†"]
    for Remove in ToRemove:
        research_topic=research_topic.replace(Remove,"")
    research_topic=research_topic.replace("-"," ")
    research_topic = ''.join([i for i in research_topic if not i.isdigit()])
    research_topic = research_topic.strip()
    research_topic = research_topic.rstrip()
    return research_topic;

def GetSubjectAreas(Conference):
    if(Conference[3]!=None):
        ConferenceSearchID=Conference[3]
    time.sleep(3)
    SearchURL="http://portal.core.edu.au/conf-ranks/"+ConferenceSearchID+"/"
    r = requests.get(SearchURL)
    soup = BeautifulSoup(r.content,'html.parser')
    details = soup.find_all("div",id = "detail" )
    i = 0
    for heading in details:
        divs= heading.find_all('div', class_ ='evenrow')
        research_topic = divs[2].get_text().strip()
    research_topic=CleanSubjectArea(research_topic)
    return [research_topic,]

def GetConferenceID(ConAcronym):
    Sql="SELECT ConID FROM `conference` WHERE ConAcronym=%s"
    val=(ConAcronym,)
    ConferenceID=DBSelectWithValues(Sql,val)
    return ConferenceID[0][0]

def GetSubjectAreaID(Subject):
    Sql="SELECT SubID FROM `subjectarea` WHERE SubName=%s"
    val=(Subject,)
    SubjectID=DBSelectWithValues(Sql,val)
    return SubjectID[0][0]

def isUniqueSaveConference(ConferenceID,SubjectID):
    Sql="SELECT ConID FROM `conferencesubject` WHERE ConID=%s and SubID=%s;;"
    val=(ConferenceID,SubjectID)
    ConferenceID=DBSelectWithValues(Sql,val)
    if(len(ConferenceID)==0):
        return True
    else:
        return False

def saveConferenceSubject(Conference,Subjects):
    ConferenceID=GetConferenceID(str(Conference[1]))
    for Subject in Subjects:
        SubjectID=GetSubjectAreaID(Subject)
        if(isUniqueSaveConference(ConferenceID,SubjectID)==True):
            Sql="INSERT INTO `conferencesubject` (`ConID`, `SubID`) VALUES (%s, %s)"
            val=(ConferenceID,SubjectID)
            try:
                DBInsert(Sql,val)
            except:                
                print("saveConferenceSubject Data Insertion Failed")


def isUniqueSubjectArea(subject):
    Sql="SELECT SubID FROM `subjectarea` WHERE SubName=%s;"
    val=(subject,)
    SubjectID=DBSelectWithValues(Sql,val)
    if(len(SubjectID)==0):
        return True
    else:
        return False

def SaveSubjectAreas(Conference,Subjects):
    StartID=4000000
    for subject in Subjects:
        if(isUniqueSubjectArea(subject)==True):
            ID=DBSelect("SELECT MAX(SubID) FROM `subjectarea`") 
            NextID=GetNewID(ID,StartID)
            Sql="INSERT INTO `subjectarea` (`SubID`, `SubName`) VALUES (%s, %s)"
            val=(NextID,subject)
            try:
                DBInsert(Sql,val)
            except:                
                print("SaveSubjectAreas Data Insertion Failed")
        

def GetConferenceFromDB(PaperTitle):
    Sql="SELECT ConID FROM `conference` where ConName=%s;"
    val=(PaperTitle,)
    ConferenceID=DBSelectWithValues(Sql,val)
    return ConferenceID[0][0]

def SavePublications(Conference,PaperTitle,PaperType,PaperYear):
    DBConfereceID=GetConferenceFromDB(str(Conference[0]))
    StartID=5000000
    ID=DBSelect("SELECT MAX(PubID) FROM `publication`") 
    NextID=GetNewID(ID,StartID)
    Sql="INSERT INTO `publication` (`PubID`, `PubName`, `PubType`, `PubYear`, `ConID`) VALUES (%s, %s, %s, %s, %s)"
    val=(NextID,PaperTitle,PaperType,PaperYear,DBConfereceID)
    try:
        DBInsert(Sql,val)
    except:                
        print("SavePublications Data Insertion Failed")        


def SavePublicationsWithoutConference(PaperTitle,PaperType,PaperYear):
    StartID=5000000
    ID=DBSelect("SELECT MAX(PubID) FROM `publication`") 
    NextID=GetNewID(ID,StartID)
    Sql="INSERT INTO `publication` (`PubID`, `PubName`, `PubType`, `PubYear`, `ConID`) VALUES (%s, %s, %s, %s,NULL)"
    val=(NextID,PaperTitle,PaperType,PaperYear)
    try:
        DBInsert(Sql,val)
    except:                
        print("SavePublicationsWithoutConference Data Insertion Failed")        


def isConferenceUnique(Conference):
    Sql="SELECT ConID FROM `conference` WHERE ConAcronym=%s"
    val=(Conference[1],)
    ConferenceID=DBSelectWithValues(Sql,val)
    if(len(ConferenceID)==0):
        return True
    else:
        return False
    
def SaveConference(Conference):
    if(isConferenceUnique(Conference)==True):
        StartID=3000000
        ID=DBSelect("SELECT Max(ConID) FROM `conference`")
        NextID=GetNewID(ID,StartID) 
        Sql="INSERT INTO `conference` (`ConID`, `ConName`, `ConAcronym`, `ConRank`, `OrgID`) VALUES (%s, %s, %s, %s,NULL);"
        val=(NextID,Conference[0],Conference[1],Conference[2])
        try:
            DBInsert(Sql,val)
        except:                
            print("Save Conference Data Insertion Failed")


def GetConference(PaperType,PaperVenue):
    if(PaperType=="Conference and Workshop Papers"):
        data= pd.read_csv("CORE.csv")
        index=data[data['Acronym']==PaperVenue].index.values
        if(len(index)>0):
            ConferenceTitle=str(data.loc[index[0]]['Title'])
            ConferenceRank=str(data.loc[index[0]]['Rank'])
            CoreRankID=str(data.loc[index[0]]['ID'])
            ConferenceInfo=[ConferenceTitle,PaperVenue,ConferenceRank,CoreRankID]
            return ConferenceInfo
        else:
            return 0
    else:
        return 0

def getAuthorID(Author):
    Sql="SELECT AuthorID FROM `author` where FirstName=%s and Surname=%s"
    val=(Author[1],Author[2])
    AuthorID=DBSelectWithValues(Sql,val)
    return AuthorID[0][0]

def getPaperID(PaperTitle,PaperType,PaperYear):
    Sql="SELECT PubID FROM `publication` WHERE PubName=%s and PubType=%s and PubYear=%s"
    val=(PaperTitle,PaperType,PaperYear)
    PaperID=DBSelectWithValues(Sql,val)
    return PaperID[0][0]

def isUniqueAuthorPublication(AuthorID,PublicationID):
    Sql="SELECT AuthorID FROM `authorpublication` WHERE AuthorID=%s and PubID=%s"
    val=(AuthorID,PublicationID)
    AuthorID=DBSelectWithValues(Sql,val)
    if(len(AuthorID)==0):
        return True
    else:
        return False

def SaveAuthorPublications(Author,PaperTitle,PaperType,PaperYear):
    AuthorID=getAuthorID(Author)
    PublicationID=getPaperID(PaperTitle,PaperType,PaperYear)
    Sql="INSERT INTO `authorpublication` (`AuthorID`, `PubID`) VALUES (%s, %s)"
    try:
        if(isUniqueAuthorPublication(AuthorID,PublicationID)==True):
            val=(AuthorID,PublicationID)         
            try:
                DBInsert(Sql,val)
            except:                               
                print("SaveAuthorPublications Data Insertion Failed")
    except:
        return 0

    
def getPublicationsandConferences(GroupName):
    Authors=GetAuthors(GroupName)
    for Author in Authors:
        response = requests.get("https://dblp.org/search/publ/api?q="+str(Author[1])+"$"+str(Author[2])+"$"+"&h=1000")
        if(response):
            tree = ET.ElementTree(ET.fromstring(response.text))
            root = tree.getroot()
            for hit in root.iter('hits'):
                TotalHits=hit.attrib
                HitCount=int(TotalHits["total"])
            if(HitCount<=1000):
                AuthorFound=False
                for hits in root.findall('hits'):  
                    for hit in hits.findall('hit'):
                        for info in hit.findall('info'):                        
                            for authors in info.findall('authors'):
                                for PaperAuthor in authors.findall('author'):
                                    if(str(PaperAuthor.text).lower()==str(Author[1]+" "+Author[2]).lower()):
                                        AuthorFound=True
                        if(AuthorFound==True): 
                            if(info.find('title')!=None):
                                PaperTitle=info.find('title').text
                            if(info.find('venue')!=None):
                                PaperVenue=info.find('venue').text
                            if(info.find('type')!=None):
                                PaperType=info.find('type').text
                            if(info.find('year')!=None):
                                PaperYear=info.find('year').text 
                            Respond=CompareAuthorPublicationsWithDb(PaperTitle,PaperType,PaperYear)
                            if(Respond==True):   
                                Conference=GetConference(PaperType,PaperVenue)
                                if(Conference!=0):
                                    Subjects=GetSubjectAreas(Conference)
                                    SaveConference(Conference)
                                    SaveSubjectAreas(Conference,Subjects)
                                    saveConferenceSubject(Conference,Subjects)
                                    SavePublications(Conference,PaperTitle,PaperType,PaperYear)                                    
                                else:
                                    SavePublicationsWithoutConference(PaperTitle,PaperType,PaperYear)
                                #sys.exit()
                            SaveAuthorPublications(Author,PaperTitle,PaperType,PaperYear)  
                            

def main():
    T1=['APO','CISO','https://www.irit.fr/departement/calcul-intensif-simulation-optimisation/equipe-apo/','France']
    T2=['REVA','CISO','https://www.irit.fr/departement/calcul-intensif-simulation-optimisation/reva/','France']
    T3=['IRIS','GD','https://www.irit.fr/departement/gestion-de-donnees/iris/','France']
    T4=['PYRAMID','GD','https://www.irit.fr/departement/gestion-de-donnees/equipe-pyramide/','France']
    T5=['GIS','GD','https://www.irit.fr/departement/gestion-des-donnees/equipe-sig/','France']
    T6=['ADRIA','IA','https://www.irit.fr/departement/intelligence-artificielle/adria/','France']
    T7=['LILaC','IA','https://www.irit.fr/departement/intelligence-artificielle/equipe-lilac/','France']
    T8=['MELODI','IA','https://www.irit.fr/departement/intelligence-artificielle/equipe-melodi/','France']
    T9=['ELIPSE','ICI','https://www.irit.fr/departement/intelligence-collective-interaction/elipse/','France']
    T10=['SMAC','ICI','https://www.irit.fr/departement/intelligence-collective-interaction/equipe-smac/','France']
    T11=['TALENT','ICI','https://www.irit.fr/departement/intelligence-collective-interaction/talent/','France']
    T12=['ACADIE','FSL','https://www.irit.fr/departement/fiabilite-des-systemes-et-des-logiciels/equipe-acadie/','France']
    T13=['ARGOS','FSL','https://www.irit.fr/departement/fiabilite-des-systemes-et-des-logiciels/equipe-argos/','France']
    T14=['SMART','FSL','https://www.irit.fr/departement/fiabilite-des-systemes-et-des-logiciels/smart/','France']
    
    Teams=[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14]
    #Teams=[T1,T2,T3]
    for Team in Teams:
        SubmitIRITDepartment(str(Team[0]),str(Team[1]),str(Team[2]),str(Team[3]))
        getPublicationsandConferences(str(Team[0]))
        time.sleep(5)

main()

