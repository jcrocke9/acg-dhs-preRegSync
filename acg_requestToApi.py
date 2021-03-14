import pyodbc
import requests
from requests.auth import HTTPBasicAuth
import re
import datetime
import json
import pandas
from decouple import config

apikey = config("API_KEY")
apipassword = config("API_SECRET")
resource = config("RESOURCE")
server = config("server")
database = config("database")
table = config("table")
username = config("username")
password = config("password")
driver = '{ODBC Driver 17 for SQL Server}'
residential_ziphealthdistrict = config('residential_ziphealthdistrict')

fileLocationList = [
    {
        'matchesLocation': 'C:/preregistrants/2021-03-14/matches.xlsx',
        'matchesSheetName': 'Matches',
        'errorsLocation': 'C:/preregistrants/2021-03-14/erros.xlsx',
        'errorsSheetName': 'Errors'
    }
]

# labels from Socrata API that may change
label = {
    'source': 'VaxVA_API',
    'vaxva_unique_id': 'csid',
    'vaxva_firstname': 'preregsurvey_first_name',
    'vaxva_lastname': 'preregsurvey_last_name',
    'vaxva_email': 'preregsurvey_email',
    'vaxva_phone': 'preregsurvey_phone_number',
    'vaxva_birthdate': 'preregsurvey_date_of_birth',
    'vaxva_eligibilitycategory': 'preregsurvey_individual_has_condition',
    'vaxva_preregdate': 'preregsurvey_registration_date',
    'vaxva_address1_line1': 'preregsurvey_residential_address1',
    'vaxva_address1_postalcode': 'preregsurvey_residential_zip_code',
    'VIIS_hasVaccination': 'viis_hasvaccination'
}

# RegEx patterns used:
patternName = '[0-9,A-Z,a-z," ","\-",",",".",r"\'"]+'
patternNameJust = '([A-Z])\w+'
patternEmail = '[0-9,A-Z,a-z," ","@","\-",".","_", "+"]+'
patternAddy1 = '[0-9,A-Z,a-z," ","\-",",",".","#"]+'
patternZIP = '[0-9]{5}'
patternPhone = '[0-9]+'

print('starting sql:', datetime.datetime.now())
conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password)
query = "SELECT acg_anchor, VIIS_anchor FROM " + table
sqlDf = pandas.read_sql(query,conn)
#sqlList = []
sqlListActualAnchors = []
for index, row in sqlDf.iterrows():
    if row[0] is not None:
        #sqlList.append(row[0])
        if '@' in row[0]:
            sqlListActualAnchors.append(row[0])
#print('sqlList:', len(sqlList))
print('sqlListActualAnchors:', len(sqlListActualAnchors))
print('ended sql, starting download:', datetime.datetime.now())

filterdate = '2021-03-13T00:00:00' # for a range: "2021-03-08T00:00:00' AND preregsurvey_registration_date <= '2021-03-13T00:00:00" # 
limit = 1000
offset = 1000
url = resource + "?$limit=" + str(limit) + "&$offset=0&$where=preregsurvey_registration_date >= '" + filterdate + "' AND preregsurvey_residential_ziphealthdistrict = '" + residential_ziphealthdistrict + "'"
response = requests.get(url, auth=HTTPBasicAuth(apikey, apipassword))
list_response = json.loads(response.text)
apiResultsDf = pandas.DataFrame(list_response)

# if limit was reached, let's keep on goin!
if len(apiResultsDf) == limit:
    while True:
        print('calling:', str(offset / 1000))
        url = resource + "?$limit=" + str(limit) + "&$offset=" + str(offset) + "&$where=preregsurvey_registration_date >= '" + filterdate + "' AND preregsurvey_residential_ziphealthdistrict = '" + residential_ziphealthdistrict + "'"
        response1 = requests.get(url, auth=HTTPBasicAuth(apikey, apipassword))
        list_response1 = json.loads(response1.text)
        df1 = pandas.DataFrame(list_response1)
        apiResultsDf = apiResultsDf.append(df1, ignore_index=True)
        offset = offset + limit
        # if limit wasn't reached, let's keep on goin!
        if len(df1) < limit:
            break
print('apiResultsDf:', len(apiResultsDf))
print('ended download, starting records:', datetime.datetime.now())
apiRecords = []
for index, row in apiResultsDf.iterrows():
    #print(row)
    ln = '' # vaxva_lastname
    em = ''  # emailaddress1
    ec = ''  # acgvax_eligibilitycategory
    rd = ''  # vaxva_preregdate
    addy1 = ''  # address1_line1
    pc = ''  # vaxva_address1_postalcode
    tel = '' # vaxva_phone
    dob1 = ''  # date of birth misc
    if isinstance(row[label['vaxva_firstname']], str):
        if row[label['vaxva_firstname']]:
            fnm = re.match(patternName, row[label['vaxva_firstname']], flags=0)
            if fnm is not None:
                fn = fnm.group(0).replace("'", "''")
                justFirstName = re.match(
                    patternNameJust, fnm.group(0), flags=0)
                if justFirstName is not None:
                    fn = justFirstName.group(0).replace("'", "''")

    if isinstance(row[label['vaxva_lastname']], str):
        if row[label['vaxva_lastname']]:
            lnm = re.match(patternName, row[label['vaxva_lastname']], flags=0)
            if lnm is not None:
                ln = lnm.group(0).replace("'", "''")

    if isinstance(row[label['vaxva_email']], str):
        if row[label['vaxva_email']]:
            emm = re.match(patternEmail, row[label['vaxva_email']], flags=0)
            if emm is not None:
                em = emm.group(0)
    # registration_date_time
    if isinstance(row[label['vaxva_preregdate']].replace(".000",""), str):
        if len(row[label['vaxva_preregdate']].replace(".000","")) == 19:
            dto = datetime.datetime.strptime(
                row[label['vaxva_preregdate']].replace(".000",""), '%Y-%m-%dT%H:%M:%S')
            rd = str(dto.replace(tzinfo=datetime.timezone.utc).isoformat())
    # vaxva_birthdate
    if isinstance(row[label['vaxva_birthdate']].replace(".000",""), str):
        if len(row[label['vaxva_birthdate']].replace(".000","")) == 19:
            dto1 = datetime.datetime.strptime(
                row[label['vaxva_birthdate']].replace(".000",""), '%Y-%m-%dT%H:%M:%S')
            dob1 = str(dto1.replace(
                tzinfo=datetime.timezone.utc).isoformat())
    # vaxva_address1_postalcode
    if isinstance(row[label['vaxva_address1_postalcode']], int):
        pcs = str(row[label['vaxva_address1_postalcode']])
        pc = pcs[0:5]
    elif isinstance(row[label['vaxva_address1_postalcode']], float):
        pcs = str(row[label['vaxva_address1_postalcode']])
        pc = pcs[0:5]
    elif isinstance(row[label['vaxva_address1_postalcode']], str):
        if row[label['vaxva_address1_postalcode']]:
            pcm = re.search(
                patternZIP, row[label['vaxva_address1_postalcode']], flags=0)
            if pcm is not None:
                pc = pcm.group(0)
    # vaxva_address1_line1
    if isinstance(row[label['vaxva_address1_line1']], str):
        if row[label['vaxva_address1_line1']]:
            addy1m = re.match(
                patternAddy1, row[label['vaxva_address1_line1']], flags=0)
            if addy1m is not None:
                addy1 = addy1m.group(0)

    if isinstance(row[label['vaxva_phone']], int):
        tel = str(row[label['vaxva_phone']])
    elif isinstance(row[label['vaxva_phone']], float):
        tel = str(row[label['vaxva_phone']]).replace(".0", "")
    elif isinstance(row[label['vaxva_phone']], str):
        if row[label['vaxva_phone']]:
            telm = re.search(
                patternPhone, row[label['vaxva_phone']], flags=0)
            if telm is not None:
                tel = telm.group(0)

    # this is to see if the client reports a medical condition:
    medicalCondition = 'No'
    if row['preregsurvey_individual_has_condition_asthma'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_cancer'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_cerebrovascular_disease'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_chronic_kidney_disease'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_copd'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_cystic_fibrosis'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_down_syndrome'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_heart_conditions'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_hypertension'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_immunocompromised'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_liver_disease'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_neurologic_conditions'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_obesity'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_overweight'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_pregnancy'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_pulmonary_fibrosis'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_severe_obesity'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_sickle_cell_disease'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_smoking'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_thalassemia'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_type_1_diabetes_mellitus'] == 'Yes':
        medicalCondition = 'Yes'
    if row['preregsurvey_individual_has_condition_type_2_diabetes_mellitus'] == 'Yes':
        medicalCondition = 'Yes'

    anchor = ln.lower().replace("'", "") + fn.lower().replace("'", "") + em.lower()
    if "@" in anchor:
        record = {
            'acg_anchor': anchor,
            'vaxva_unique_id': row['csid'],
            'vaxva_firstname': fn,
            'vaxva_lastname': ln,
            'vaxva_email': em,
            'vaxva_phone': tel,
            'vaxva_birthdate': dob1,
            'vaxva_preregdate': rd,
            'vaxva_address1_line1': addy1,
            'vaxva_address1_postalcode': pc,
            'VIIS_hasVaccination': row[label['VIIS_hasVaccination']],
            'vaxva_eligibilitycategory': medicalCondition
        }
        apiRecords.append(record)
print('apiRecords:', len(apiRecords))
print('ended records, starting matching:', datetime.datetime.now())
matches = []
errorsFromInsert = []
inserts = 0
for apiItem in apiRecords:
    inSql = apiItem['acg_anchor'] in sqlListActualAnchors
    if inSql:
        matches.append(apiItem)
    else:
        try:
            query = "INSERT INTO " + table + " (acg_anchor,acg_updated,acg_firstname,acg_lastname,acg_emailaddress1,vaxva_unique_id,vaxva_firstname,vaxva_lastname,vaxva_email,vaxva_phone,vaxva_birthdate,vaxva_eligibilitycategory,vaxva_preregdate,vaxva_address1_line1,vaxva_address1_postalcode,VIIS_hasVaccination,acg_created) " + \
                "VALUES (N'" + \
                apiItem["acg_anchor"] + "', " + \
                "CURRENT_TIMESTAMP, N'" + \
                apiItem["vaxva_firstname"] + "', N'" + \
                apiItem["vaxva_lastname"] + "', N'" + \
                apiItem["vaxva_email"] + "', N'" + \
                apiItem["vaxva_unique_id"] + "', N'" + \
                apiItem["vaxva_firstname"] + "', N'" + \
                apiItem["vaxva_lastname"] + "', N'" + \
                apiItem["vaxva_email"] + "', N'" + \
                apiItem["vaxva_phone"] + "', N'" + \
                apiItem["vaxva_birthdate"] + "', N'" + \
                apiItem["vaxva_eligibilitycategory"] + "', N'" + \
                apiItem["vaxva_preregdate"] + "', N'" + \
                apiItem["vaxva_address1_line1"] + "', N'" + \
                apiItem["vaxva_address1_postalcode"] + "', N'" + \
                apiItem["VIIS_hasVaccination"] + "', " + \
                "CURRENT_TIMESTAMP" + \
                ")"
            with conn.cursor() as cursor:
                cursor.execute(query)
            inserts = inserts + 1
        except:
            print(apiItem)
            errorsFromInsert.append(apiItem)

print('ending matching:', datetime.datetime.now())
print('matches:', len(matches))
print('inserts:', inserts)
# output:
matchesDf = pandas.DataFrame(matches)
matchesDf.to_excel(fileLocationList[0]['matchesLocation'],
                    sheet_name=fileLocationList[0]['matchesSheetName'])
# output:
errorsFromInsertDf = pandas.DataFrame(errorsFromInsert)
errorsFromInsertDf.to_excel(fileLocationList[0]['errorsLocation'],
                    sheet_name=fileLocationList[0]['errorsSheetName'])