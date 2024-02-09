from datetime import datetime
import re
from helper import * 
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set up file handler
handler = logging.FileHandler(f'extracts/logs/log.txt')
handler.setLevel(logging.INFO)

# Set up formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
min_salary =''
max_salary  =''
# Add handler to logger
logger.addHandler(handler)
### THIS SECTION IS FOR EXTRACTION FROM SEEK ###

################################################################################

def reset():
    with open('seek_linind_comp.txt', 'w') as file:
        file.write(str(0))
    with open('seek_urlind_comp.txt', 'w') as file:
        file.write(str(0))
    timestamp = datetime.now().date()
    with open('latest_extract_timestamp.txt', 'w') as file:
        file.write(str(timestamp))

################################################################################

def extract(): # For every URL and corresponding page
    # for linkSet in Links:
    #     for url in linkSet:
    
    with open('seek_linind_comp'+'.txt', 'r') as file:
        linind = int(file.read())
    with open('seek_urlind_comp'+'.txt', 'r') as file:
        urlind = int(file.read())
        
    for lin in range(linind,len(Links)):
        linkSet = Links[lin]
        for ur in range(urlind,len(linkSet)):
            url = linkSet[ur]
            jobList = []
            name = getSubstring(str(url), 'jobs-in-','?')
            salary_range = getSubstring(str(url), 'salaryrange=', '&').replace('-','_')
            outputName = f'{name}${salary_range}'
            categoryString= (re.sub("-", " ",url[32:]))
            category = categoryString.split('?')[0]
            numRecords = extract_num_records(url)
            numPages = int(numRecords/21) # Since 21 records per page
            # Extract min_salary and max_salary using regex
            print(numRecords)
            print(numPages)
            logger.info(f'Starting {outputName}')
            if(numPages<1): # To read page 0
                pageTransform(1,1,jobList,category,url)
            # each page i till numPages
            for i in range(1, numPages+1): 
                pageTransform(i,numPages,jobList,category,url)
            df=pd.DataFrame(jobList)
            #  print(df)
            df.to_csv(f'extracts/testing/{outputName}.csv')
            with open('seek_urlind_comp'+'.txt', 'w') as file:
                file.write(str(urlind+1))
        with open('seek_linind_comp'+'.txt', 'w') as file:
            file.write(str(linind+1))
            
################################################################################

def pageTransform(i,numPages,jobList,category,url):
        loadingUIv2(i,numPages)
        # find the index of the first question mark in the URL
        qm_index = url.find('?')
        url_addon = url[:qm_index+1] + f'page={int(i)}&' + url[qm_index+1:]
        r=requests.get(url_addon,headers, timeout=50)
        soup=BeautifulSoup(r.content,'html.parser')
        transform(soup, jobList,category)   

################################################################################
# HELPER FUNCS - COMMON

def loadingUI(i, MAX):
    if i%int(MAX/100) == 0:
        str = '|'
        print(str, end ="")
    if(i==MAX):
        print('')

def loadingUIv2(i, MAX):
    perc = int((i/MAX)*100)
    #clear_output(wait=True)
    str = '|'*+perc+f'{perc}%'
    print(str)
    print(f'Done {i} of {MAX}')

def getSubstring(str, startString, endString):
    x = str.index(startString) + len(startString)
    if endString  != '':
        y = str[x:].index(endString)+x
    else:
        y = len(str)
    return(str[x:y])
################################################################################
# Extracts number of records given the URL of a category 
def extract_num_records(url):
    r=requests.get(url,headers)
    try:
        substr = getSubstring(str(r.content), 'Find your ideal job at SEEK with ', ' jobs')
        numResults = int(re.sub(",", "", substr))
    except: 
        numResults = 1
    return numResults

# Function to convert date
def convert_to_date(relative_time_str):
    now = datetime.now()
    # Extract the number and unit (e.g., '5d', '3hr', '39min')
    if "d" in relative_time_str:
        days = int(relative_time_str.split("d")[0])
        date = now - timedelta(days=days)
    elif "hr" in relative_time_str:
        hours = int(relative_time_str.split("hr")[0])
        date = now - timedelta(hours=hours)
    elif "min" in relative_time_str:
        minutes = int(relative_time_str.split("min")[0])
        date = now - timedelta(minutes=minutes)
    else:
        raise ValueError("Unsupported format!")
    return date.date()

# Transforms soup from seek to corresponding dataframe
def transform(soup, jobList,category):
    
    #divs=soup.find_all('div', class_='yvsb870 v8nw070 v8nw072') # UPDATE THIS
    parentDivs =soup.find_all('div', class_ = itemsParentDiv)
    # class="_1wkzzau0 szurmz0 szurmzb"
    for parent in parentDivs:
    # print(f'HERE IS THE divs: {divs}')
            # For each parent, fetch child divs with the specified class
        try:    
            divs = parent.find_all('div', class_=itemsDiv)
        except:
            print('error in divs')
        for item in divs:
            try:
                #jobTitle=item.find('a', {'data-automation':'jobTitle'}).text.strip()
                #jobTitle = f"'{item.find('a', {'data-automation':'jobTitle'})}'"
                
                # if("promoted" in jobTitle):
                #     print(jobTitle)
                #     continue

                jobTitleLink = item.find('a', {'data-automation': 'jobTitle'})
                jobTitle = jobTitleLink.text.strip()
                jobHref = jobTitleLink['href']  # Extracting href
                #print(jobTitleLink)
            except:
                jobTitle= ''

            try:
                companyName=item.find('a', {'data-automation':'jobCompany'}).text.strip()
            except:
                companyName= ''
            #print(CompanyNmae)
            
            try:
                jobSalary=item.find('span', {'data-automation':'jobSalary'}).text.strip()
            except:
                jobSalary= '-'
            
            try:
                jobLocationAll=item.find_all('a', {'data-automation':'jobLocation'})
                jobLocation = ', '.join([loc.text for loc in jobLocationAll])

                #def remove_html(jobLocationAll):
                    #print(jobLocationAll)
                #    return ','.join(xml.etree.ElementTree.fromstring(jobLocationAll).itertext())
                #jobLocation=remove_html(jobLocationAll)
                #print(jobLocation)
            except:
                jobLocation= ''
            
            try:
                jobSubClassification = item.find('a', {'data-automation': 'jobSubClassification'}).text
                #print(jobSubClassification)
            except:
                jobSubClassification= ''  

            try:
                entireDiv=item.find_all('div',{'class': itemsDiv})
                #Description = convert_relative_date_to_actual(Description, current_date_time)
                
            except:
                entireDiv= ''  
            
            try:  
                jobShortDescription = item.find('span', {'data-automation': 'jobShortDescription'}).text.strip()
                #print("Job Short Description:", jobShortDescription)
            except:
                jobShortDescription ='' 

            try:
                ago = item.find('span', {'data-automation': 'jobListingDate'}).text.strip()
                #print(ago)
                datePosted=convert_to_date(ago)

            except:
                datePosted ='' 
            
            if jobTitle and jobTitle != "'None'":
                if datePosted in [None, 'None', 'null']:
                    datePosted = datetime.now().date()
                
                job =   {
                    'link': 'https://www.seek.com.au/'+jobHref,
                    'classification': category,
                    'category':jobSubClassification,
                    'title':jobTitle,
                    'company':companyName,
                    'salary':jobSalary,
                    'min_salary': min_salary,
                    'max_salary': max_salary,
                    #'jobLocation':jobLocationAll,
                    'location':jobLocation,
                    'description':jobShortDescription,
                    'entireDiv':entireDiv,
                    'date_posted': datePosted
                }
                
                jobList.append(job)
    return len(divs)

extract()

