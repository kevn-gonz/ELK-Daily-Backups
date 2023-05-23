# This script was created/tested using python3.9. Other python versions might not work

import requests
import sys
import codecs
from datetime import date, timedelta
import re
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3

#Variables needed:
deleteDaysOld = (date.today() - timedelta(days=9)).strftime('%Y.%m.%d')
yesterday = (date.today() - timedelta(days=1)).strftime('%Y\.%m\.%d')
snapYesterday = (date.today() - timedelta(days=1)).strftime('%Y.%m.%d')
thisWeek = (date.today()).strftime('%Y\.%V')
thisMonth = (date.today()).strftime('%Y\.%m')
thisYear = (date.today()).strftime('%Y')
#Change the below base64 credentials if the elastic credential is updated
elasticAuthentication = {'Authorization': 'Basic <BASE64CREDENTIALS>'}
#5 attempts & 2 backof => 1,2,4,8,16 (delay in seconds between retries)
retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["PUT", "GET", "DELETE"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)
urllib3.disable_warnings()


#Lists needed:
#Compliance indices will be ignored on this script, they have their own script
complianceIndices = ["windows-", "compliance-", "infosec-"]
#Special indices means those indices that will be stored in a separate repository and folder.
#   usually because they are huge indices
specialIndices = ["websys-","onprem-"]
try:
    indicesList = (http.get('http://<ELASTICSEARCH_HOST>:9200/_cat/indices/*,-shrink*,-.*?h=i&s=i:asc', headers=elasticAuthentication).content.decode('utf-8')).rstrip().split('\n')
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print("Something went wrong while getting the list of indices (exiting script now): Line where code broke: ", exc_tb.tb_lineno)
    print("Error exception: ", e)
    sys.exit(1)
indicesToBackup = [s for s in indicesList if (re.match(r'.*-'f'{yesterday}''$', s) or re.match(r'.*-'f'{thisWeek}''$', s) or re.match(r'.*-'f'{thisMonth}''$', s) or re.match(r'.*-'f'{thisYear}''$', s)) and not s.startswith(tuple(complianceIndices))]
departmentsToBackup = sorted(set([s.split('-')[0] for s in indicesToBackup]))
departmentsToBackup.extend(specialIndices)

clusterStatus = 0

while True:
    try:
        r = http.get('http://<ELASTICSEARCH_HOST>:9200/_cluster/health', headers=elasticAuthentication)
        r.raise_for_status()
        resp = (r).json()
        if re.search("'status': 'green'", str(resp)):
            print("Cluster is green... Starting the backups/snapshots")

            #Main cycle, here the Directories, Repositories and Snapshots will be created
            for x in departmentsToBackup:
                dirName = f'/opt/apps/asmt/elastic/backup/daily/{x}/'
                repoName = f'_snapshot/elk_backup_{x}'

                #Creating the Directory that will be linked to the repository
                try:
                    os.makedirs(dirName)
                    print("Directory: ", dirName, " created")
                except FileExistsError:
                    print("Directory: ", dirName, " already exists")
                except:
                    print("Directory ERROR: An error occurred when trying to create the directory ", dirName)
                    print("Directory ERROR: Skipping the backup for ", x)
                    continue

                #Creating the corresponding Repository
                repoExist = 0
                try:
                    r = http.get(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}', headers=elasticAuthentication)
                    r.raise_for_status()
                    repoExist = 1
                except requests.exceptions.RequestException as err:
                    if "404" in str(err.response):
                        #If the repo doesn't exist (404) it's fine, we'll create it below
                        repoExist = 0
                    else:
                        print("Repository ERROR: Could not verify if repository exist or not, skipping.. ", x)
                        continue

                if repoExist == 0:
                    try:
                        r = http.put(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}', json={"type": "fs", "settings": {"location": f'{dirName}', "compress": "true"}}, headers=elasticAuthentication)
                        r.raise_for_status()
                        print("Repository: ", repoName, " created")
                    except requests.exceptions.RequestException as err:
                        print ("Repository ERROR: ", repoName, " OOps: Something happened: ",err)
                        print ("Repository ERROR: Skipping the backup for ", x)
                        continue
                else:
                    print(f"Repository: {repoName} already exist, skipping...") 


                indexSnap = ','.join(map(str, [s for s in indicesToBackup if s.startswith(x) and not s.startswith(tuple(specialIndices))]))
                if len(indexSnap) == 0:
                    indexSnap = ','.join(map(str, [s for s in indicesToBackup if s.startswith(x) and s.startswith(tuple(specialIndices))]))


                if len(indexSnap) == 0:
                    print ("Snapshot ERROR: An error occured and there are NO indices to backup for ", x)
                    continue
                else:
                    try:
                        r = http.put(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}/snapshot-{x}-{snapYesterday}?wait_for_completion=false', json={"indices": f"{indexSnap}", "ignore_unavailable": "true", "include_global_state": "false"}, headers=elasticAuthentication)
                        r.raise_for_status()
                        print(f"Snapshot: {repoName}/snapshot-{x}-{snapYesterday} completed")
                    except requests.exceptions.RequestException as err:
                        if "400" in str(err.response):
                            print (f"Snapshot: {repoName}/snapshot-{x}-{snapYesterday} Code 400 - An snapshot with that name might already exist.")
                        else:
                            print (f"Snapshot ERROR: {repoName}/snapshot-{x}-{snapYesterday} OOps: Something happened: ", err)
                            continue

                #Checking if the last snapshot is still in progress, if it is, wait until it is over
                while True:
                    try:
                        r = http.get(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}/snapshot-{x}-{snapYesterday}', headers=elasticAuthentication)
                        r.raise_for_status()
                        resp = (r).json()
                        if re.search("'state': 'IN_PROGRESS'", str(resp)):
                            time.sleep(30)
                        else:
                            break
                    except requests.exceptions.RequestException as err:
                        print (f"Snapshot ERROR: {repoName}/snapshot-{x}-{snapYesterday} OOps: Something happened: ", err)
                        break

            print("No more pending backups!")
            time.sleep(2)

            ################################# CONFIRM EVERYTHING WORKED AS EXPECTED #####################################

            #Code to check if the backup was done or not, send passive check to Icinga if the backup failed
            bakCompleted = 0
            for x in departmentsToBackup:
                repoName = f'_snapshot/elk_backup_{x}'
                try:
                    r = http.get(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}/snapshot-{x}-{snapYesterday}', headers=elasticAuthentication)
                    r.raise_for_status()
                    resp = (r).json()
                    if re.search("'state': 'SUCCESS'", str(resp)):
                        bakCompleted += 1
                except requests.exceptions.RequestException as err:
                    print (f"Final Confirmation ERROR: {repoName}/snapshot-{x}-{snapYesterday} OOps: Something happened: ", err)

            if len(departmentsToBackup) == bakCompleted:
                print("Backup SUCCESSFUL")
                http.get('https://<ICINGA_HOST>:8443/icingaps/generic_alert_post.php?token=<TOKEN>&host=<HOST>&service=elasticsearch_daily_backup_check&status=0&text=SUCCESS', verify=False)
            else:
                print("One or more backups FAILED")
                http.get('https://<ICINGA_HOST>:8443/icingaps/generic_alert_post.php?token=<TOKEN>&host=<HOST>&service=elasticsearch_daily_backup_check&status=2&text=FAILED', verify=False)


            #Delete 9 days old snapshots
            for x in departmentsToBackup:
                repoName = f'_snapshot/elk_backup_{x}'
                try:
                    r = http.delete(f'http://<ELASTICSEARCH_HOST>:9200/{repoName}/snapshot-{x}-{deleteDaysOld}', headers=elasticAuthentication)
                    r.raise_for_status()
                    print(f"Delete Snapshot: {repoName}/snapshot-{x}-{deleteDaysOld} completed")
                except requests.exceptions.RequestException as err:
                    if "404" in str(err.response):
                        print(f"Delete Snapshot: {repoName}/snapshot-{x}-{deleteDaysOld} doesn't exist")
                    else:
                        print (f"Delete Snapshot ERROR: {repoName}/snapshot-{x}-{deleteDaysOld} OOps: Something happened: ", err)

            #Main break, for the cluster health
            break
        else:
            clusterStatus += 1
            time.sleep(300)

    except requests.exceptions.RequestException as err:
        print (f"Cluster green ERROR: Something happened when trying to get the cluster health: ", err)
        clusterStatus += 1
        time.sleep(10)

    if clusterStatus == 10:
        print (f"Cluster green ERROR: The cluster hasn't been green for the last hour")
        http.get('https://<ICINGA_HOST>:8443/icingaps/generic_alert_post.php?token=<TOKEN>&host=<HOST>&service=elasticsearch_daily_backup_check&status=2&text=TheScriptDidNotRun-ClusterIsNOTGreen', verify=False)
        break

print("End of script, exiting now.")