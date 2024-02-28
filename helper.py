from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree
import os
#from IPython.display import clear_output
import requests
from time import sleep
import pandas as pd
import time

salary_increment = 5000
Links = []
# Timestamp
curTime = time.strftime("%Y%m%d-%H%M%S") 
curDay = date.today()
itemsParentDiv = 'y735df0 _1akoxc50 _1akoxc54'
itemsDiv = 'y735df0 _1iz8dgs6m'
################################################################################
# HELPER FUNCS - COMMON

def loadingUIv2(i, MAX):
    perc = int((i/MAX)*100)
    #clear_output(wait=True)
    str = '|'*+perc+f'{perc}%'
    print(str)


def getSubstring(str:str, startString:str, endString:str, n=1, checkInt = False, flag=1):
    '''Finds a substring when given:
    String
    Start: if '' then from the beginning
    End: if '' then till the end
    n: returns the nth occurence of the substring
    checkInt: Set true if we are parsing for salary
    flag: Ignore. Created for recursion when n>1
'''
    try:
        if startString  == '':
            x = 0
        else:
            x = str.index(startString) + len(startString)
            if (checkInt == True):
                if str[1].isdigit() == False and flag == 1: # For cases like $neg
                    return None
        if endString  != '':
            if(checkInt):
                arr = list(str[x:])
                for i in arr:
                    if(i.isdigit() == False and i != '.' and i != ',' and i.lower() != 'k'):
                        y = arr.index(i) + x
                        break
                    y = len(str)
            else:
                y = str[x:].index(endString)+x
        else:
            y = len(str)
        if(n != 1):
            return (getSubstring(str[y:], startString, endString,n= n-1,checkInt=checkInt,flag=0))
        return(str[x:y])
    except Exception as e:
        return None

# print(getSubstring('$90,000-$125990.24','$',' ',n=2, checkInt=True))

def create_links(links, urls): ## Creates an array of arrays ([[URLS in accounting], [URLS in ...],etc.])
    for url in urls:
        sector = []
        for salaryRange in salaryRanges:
            sector.append(url+salaryRange)
        links.append(sector)


################################################################################
## PREPROCESS
filePath = 'F:/Indradeep/seek-salary/extracts/testing/merged'
keywordsSydney = ['Sydney']
keywordsMelbourne = ['Melbourne']
keywordsBrisbane = ['Brisbane']
keywordsAdelaide = ['Adelaide']
keywordsGoldCoast = ['Gold Coast']
keywordsWollongong = ['Wollongong']
keywordsHobart = ['Hobart']
keywordsPerth = ['Perth']
keywordsCanberra = ['Canberra']
keywordsNewcastle= ['Newcastle']
keywordsDarwin = ['Darwin']
keywordsNSW = ['NSW']
keywordsQLD = ['QLD']
keywordsVIC = ['VIC']
keywordsTAS = ['TAS']
keywordsNT = ['NT']
keywordsWA = ['WA']
keywordsSA = ['SA']

keywordsLocations = [keywordsSydney, keywordsMelbourne, keywordsBrisbane,keywordsAdelaide,
                        keywordsGoldCoast, keywordsWollongong, keywordsHobart,
                        keywordsPerth, keywordsCanberra,keywordsNewcastle, keywordsDarwin,
                        keywordsNSW, keywordsQLD,keywordsVIC,keywordsTAS,keywordsNT, keywordsWA, keywordsSA]
#locations = ['Sydney NSW', 'Melbourne VIC', 'Brisbane QLD', 'Adelaide SA', 'Gold Coast QLD', 
#                'Wollongong NSW', 'Hobart TAS','Perth WA', 'Canberra ACT', 'Newcastle NSW', 'Darwin NT',
#                'Other NSW', 'Other QLD', 'Other VIC','Other TAS', 'Other NT', 'Other WA', 'Other SA']

locations = ['Sydney NSW', 'Melbourne VIC', 'Brisbane QLD', 'Adelaide SA', 'Gold Coast QLD', 
                'Wollongong NSW', 'Hobart TAS','Perth WA', 'Canberra ACT', 'Newcastle NSW', 'Darwin NT',
                'Other']

################################################################################
## EXTRACTION
headers={'User-Agen':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}

accountingURL="https://www.seek.com.au/jobs-in-accounting"
adminURL='https://www.seek.com.au/jobs-in-administration-office-support'
advertiseURL='https://www.seek.com.au/jobs-in-advertising-arts-media'
bankingURL='https://www.seek.com.au/jobs-in-banking-financial-services'
callCentreURL='https://www.seek.com.au/jobs-in-call-centre-customer-service'
CEOMangementURL='https://www.seek.com.au/jobs-in-ceo-general-management'
communityServicesURL='https://www.seek.com.au/jobs-in-community-services-development'
constructionURL='https://www.seek.com.au/jobs-in-construction'
consultingStrategyURL='https://www.seek.com.au/jobs-in-consulting-strategy'
designArchitectureURL='https://www.seek.com.au/jobs-in-design-architecture'
educationTrainingURL='https://www.seek.com.au/jobs-in-education-training'
engineeringURL='https://www.seek.com.au/jobs-in-engineering'
farmingURL='https://www.seek.com.au/jobs-in-farming-animals-conservation'
govtDefenceURL='https://www.seek.com.au/jobs-in-government-defence'
healthcareURL='https://www.seek.com.au/jobs-in-healthcare-medical'
hospitalityURL='https://www.seek.com.au/jobs-in-hospitality-tourism'
hrURL='https://www.seek.com.au/jobs-in-human-resources-recruitment'
ictURL='https://www.seek.com.au/jobs-in-information-communication-technology'
insuranceURL='https://www.seek.com.au/jobs-in-insurance-superannuation'
legalURL='https://www.seek.com.au/jobs-in-legal'
manufacturingURL='https://www.seek.com.au/jobs-in-manufacturing-transport-logistics'
marketingURL='https://www.seek.com.au/jobs-in-marketing-communications'
miningURL='https://www.seek.com.au/jobs-in-mining-resources-energy'
realestateURL='https://www.seek.com.au/jobs-in-real-estate-property'
retailURL='https://www.seek.com.au/jobs-in-retail-consumer-products'
salesURL='https://www.seek.com.au/jobs-in-sales'
scienceTechURL='https://www.seek.com.au/jobs-in-science-technology'
selfemploymentURL='https://www.seek.com.au/jobs-in-self-employment'
sportsURL='https://www.seek.com.au/jobs-in-sport-recreation'
tradesURL='https://www.seek.com.au/jobs-in-trades-services'


URLs = [
    accountingURL,
    adminURL ,
    advertiseURL,
    bankingURL,
    callCentreURL,
    CEOMangementURL,
    communityServicesURL,
    constructionURL,
    consultingStrategyURL,
    designArchitectureURL,
    educationTrainingURL,
    engineeringURL ,
    farmingURL,
    govtDefenceURL,
    healthcareURL,
    hospitalityURL,
    hrURL,
    ictURL,
    insuranceURL,
    legalURL,
    manufacturingURL,
    marketingURL,
    miningURL ,
    realestateURL,
    retailURL,
    salesURL,
    scienceTechURL,
    selfemploymentURL,
    sportsURL,
    tradesURL
]
salaryRanges = []

try:
    with open('latest_extract_timestamp.txt', 'r') as file:
        daterange = file.read()
except:
    daterange = 0

#[1,3,7,14,30]
if daterange != 0:    
    daterange = datetime.strptime(daterange, '%Y-%m-%d')
    today = datetime.today()
    difference = today - daterange
    daterange = difference.days
    if daterange <= 2:
        daterange = 1
    elif daterange < 7:
        daterange = 3 
    elif daterange < 14:
        daterange = 7
    elif daterange < 30:
        daterange = 14
    elif daterange <= 31:
        daterange = 30
    else:
        daterange = 0

i = 80000
while(i < 350000):
        salaryRanges.append(f'?daterange={daterange}&salaryrange={i}-{i+salary_increment}&salarytype=annual')
        i += salary_increment

create_links(Links, URLs)
print(Links)