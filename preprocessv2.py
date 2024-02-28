### THIS SECTION IS FOR PREPROCESSING ###
import statistics
from pandas import DataFrame
from helper import *
import logging
################################################################################
###############################################################################
### SETUP LOGGER ###
# Set up logger
os.makedirs(f'output/{curTime}')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Set up file handler
handler = logging.FileHandler(f'output/{curTime}/log.txt')
handler.setLevel(logging.INFO)

# Set up formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)
################################################################################
headers={'User-Agen':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
################################################################################
## PREPROCESS
filePath = r'extracts\testing'
def main_preprocess():
    print('start')
    for filename in os.scandir(filePath):
        if filename.is_file():
            logger.info(f'Started {filename.path}')
            print(f'Started {filename.name}...')
            #try:
            df=pd.read_csv(filename)
            # df = title_fix(filename)
            df = df.drop('Unnamed: 0', errors='ignore', axis=1, inplace=False)
            #df = df.drop('', axis=1, inplace=False)
            if len(df) != 0 or df.empty == False:
                df = salary_fix(df, filename.path)
                df = location_fix(df)
                # df = summary_fix(df,filename)
                df = category_fix(df,filename.name)
                df = date_fix(df)
                df = minMaxMedianSalary_fix(df)
                # getStats(df)
                df.to_csv(f'output/{curTime}/output-{filename.name}', sep=",")
                print(f'Completed {filename.name}...\n')
            else: 
                logger.info(f'DID NOT CREATE FILE: {filename} due as there is no data')
                print('EMPTY FILE')
            #except Exception as e:
                # logger.info(f'FAILED FILE: {filename} due to {e}')
                # print('FAILED FILE')
                # print(e)
    print('Complete...')
    return f'output/{curTime}'
################################################################################
def salary_fix(df, filename):
    beforeDrop = len(df)
    print(beforeDrop)
    # logger.info(f'Without Salary preprocessing: {beforeDrop}')
    rangeString = getSubstring(filename, '$', '.')
    minmax = stringToRange(rangeString)
    print(rangeString)
    for i, row in df.iterrows():
        df.at[i, 'min_salary'] = minmax[0]
        df.at[i, 'max_salary'] = minmax[1]
        
    return df

################################################################################

def title_fix(file):
    titles = []
    links = []
    df = transform_title(file,titles,links)
    df.insert(0, 'Job Title',titles,allow_duplicates=True)
    df.insert(1, 'Link',links,allow_duplicates=True)
    df = df.drop('title', axis=1, inplace=False)
    df = df.drop('Unnamed: 0', axis=1, inplace=False)
    return df
################################################################################

def location_fix(df):
    for i in range (0, len(df['location'])):
        found = False
        for j in range(0, len(locations)):
            if(not found):
                for keywordPlace in keywordsLocations[j]:
                    if (keywordPlace in df['location'].iloc[i]):
                        # sleep(1)
                        df.at[i, 'location'] = locations[j]
                        found =True
                        #print(locations[j])
                  
    return df

################################################################################
    
def transform_title(file, titles, links):
    csvData = pd.read_csv(file)
    print(f'Number of records total: {len(csvData)}')
    csvData = csvData.dropna(axis=0, subset=['title'])
    for title in csvData["title"]:
        if title == '':
            logger.info(f"EMPTY IN {title} - File unreliable")
            continue
        href = getSubstring(title, 'href="', '"')  # For http link
        if ('promoted' in title):
            logger.info(f"PROMOTED IN {title} - File unreliable")
            continue
        title = getSubstring(title, '>', '</a>')  # For title
        titles.append(title)
        # Check if href is not None before concatenating
        if href is not None:
            links.append('https://seek.com.au' + href)
        else:
            links.append(None)  # Handle the case where href is None
    return csvData

################################################################################

def summary_fix(df,filename):
    print(filename)
    logger.info(f'Started summary fix for: {filename}')
    # Read link
    summaries = []
    count = 0
    lenLinks = len(df)
    for link in df["link"] :
        try:
            r=requests.get(link,headers,timeout=40)
            soup=BeautifulSoup(r.content,'html.parser')
            # if count<100:
            #     print(f'{count}: {link}')
            transform(soup, summaries)
            loadingUIv2(count, lenLinks)
            logger.info(f'{count}: {link} - successful')
        except:
            logger.info(f'{count}: {link} - fail')
            transform(None, summaries)
        count+=1
    
    df["Summary"] = summaries
    #print(df["Summary"])
    return df
################################################################################

def transform(soup,summaries):
    try:
        summary = soup.body.find('div', {'data-automation':'jobAdDetails'}).text.strip()
        # description = soup.body.find('div', {'class':'yvsb870 v8nw070 v8nw077'}).text.strip()
    except:
        summary = ''
        print("Summary not found ")
        #print(f'{soup}')
    # print(summary +'\n\n\n')
    # print(len(summaries)+1)

    summaries.append(summary)
    return summary

################################################################################
def subCategory_list(df):
    subclassification_list = []
    for description in df['description']:
        subclassification = getSubstring(description,'subClassification: ', '<')
        subclassification_list.append(subclassification)
    return subclassification_list

################################################################################

def category_fix(df, filename:str):
    subcategories = subCategory_list(df)
    filename = getSubstring(filename,'','$')
    category_list = []
    for subclassification in subcategories:
        category = f'{filename.capitalize()} - {subclassification}'
        category = category.replace('&amp;', '&')
        category_list.append(category)
    df['Category'] = category_list
    return df

################################################################################
def date_fix(df):
    date_list = []
    for description in df['description']:
        #print(description)
        days_ago = getSubstring(description,'jobListingDate">', 'd ago')
        if(days_ago == None):
            date = curDay.strftime("%d/%m/%Y")
        else:
            date = (curDay - timedelta(days=int(days_ago))).strftime("%d/%m/%Y")
        date_list.append(date)
    df['Date'] = date_list
    return df
################################################################################
def minMaxMedianSalary_fix(df: DataFrame):
    minSalaries = []
    maxSalaries = []
    for i in range(0, len(df)):
        salary = df['salary'].iloc[i]
        defaultMin = float(df['min_salary'].iloc[i])
        defaultMax = float(df['max_salary'].iloc[i])
        if salary == '-':
            minSalaries.append(defaultMin)
            maxSalaries.append(defaultMax)
        else:
            minSalary = getSubstring(salary,'$',' ', checkInt=True)
            maxSalary = getSubstring(salary,'$',' ', n=2, checkInt=True)
            ## MIN ERROR CHECKING
            if(minSalary != None):
                try:
                    minSalary = float(salary_numerify(minSalary))
                except Exception as e: 
                    pass
                minSalary = salary_normalise(minSalary)
                minSalaries.append(minSalary) 
                # Get salary if in $***  format
            else:
                minSalaries.append(defaultMin) 
            ## MAX ERROR CHECKING
            ## If min?max - max salary = min and if it fails, max = default
            if(maxSalary != None):
                try:
                    maxSalary = float(salary_numerify(maxSalary))
                except Exception as e: 
                    pass
                maxSalary = salary_normalise(maxSalary)
                if isinstance(minSalary, (float, int)) and isinstance(maxSalary, (float, int)):
                    if maxSalary > minSalary:
                        maxSalaries.append(maxSalary)
                    else:
                        maxSalaries.append(minSalary)
                else:
                    maxSalaries.append(defaultMax)
            else:
                maxSalaries.append(defaultMax) 
    df['min_salary'] = minSalaries
    df['max_salary'] = maxSalaries  
    ## MEAN SALARY 
    meanSalary = []
    for i in range(0, len(minSalaries)):
        if isinstance(minSalaries[i], float) and isinstance(maxSalaries[i], float):
            meanSalary.append(statistics.fmean([minSalaries[i],maxSalaries[i]]))
        else:
            meanSalary.append('AVERAGE FAILED') 
    df.insert(7,'average_salary', meanSalary)  
    #print(meanSalary)
    return df
################################################################################
def salary_normalise(salary:float):
    try:
        if salary<=200: # Assuming hourly
            salary = salary*7.5*5*48
        elif salary<2000 and salary>200: # Assuming daily
            salary = salary*5*48
        elif salary<20000 and salary>2000: # Assuming monthly
            salary = salary*12
    except Exception as e:
        logger.info(f'{e} for {salary}')
    return salary
################################################################################
def salary_numerify(salary:str): # Does not convert to float though
    salary = salary.replace(',','')
    salary= salary.replace('K','000')
    salary= salary.replace('k','000')
    return salary
################################################################################

# HELPER FUNCS
def getStats(df:DataFrame):
    # 1. Num Records
    records = len(df)
    print(f'Number of records left: {records}')
    # 1. Failed MinSalaries
    failed_minSalaries = df['min_salary'].value_counts()['FAILED RETRIEVAL']
    print(f'Number of failed MIN salaries : {failed_minSalaries} || {(failed_minSalaries/records)*100}%')
    # 2. Failed MaxSalaries
    failed_maxSalaries = df['max_salary'].value_counts()['FAILED RETRIEVAL']
    print(f'Number of failed MAX salaries : {failed_maxSalaries} || {(failed_maxSalaries/records)*100}%')
    # 3. Failed MeanSalaries
    failed_meanSalaries = df['average_salary'].value_counts()['AVERAGE FAILED']
    print(f'Number of failed MEAN salaries : {failed_meanSalaries} || {(failed_meanSalaries/records)*100}%')
    # Formatting
    print(f'################\n')

def stringToRange(text:str):
    x = text.index('_') # Based on filename
    min = int(text[:x])
    max = int(text[x+1:])
    return (min,max)

################################################################################

main_preprocess()
print('start')
