import requests
import json, time
import urllib3
from manage_jobs import create_job, get_jobs, update_job_status

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

###### Job Variables
accountID=613022171325988864
datastreamID=641767662913925120
serverURL="https://200.54.255.130"
apiToken="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODE2MDM0NTUsICJlbWFpbCIgOiAicm9kcmlnby5henVhQHNnc2NtLmNsIiwgIm5hbWUiIDogInJvZHJpZ28uYXp1YUBzZ3NjbS5jbCIsICJzZXNzaW9uIiA6ICI2MTMwOTQyMzEwODc3MTAyMDgiIH0.ieBIRCnlVNQVvxDrZ0IcJ1X3vU5jsc86ll1fGbOKqnY"

# jobids=get_jobs(apiToken, serverURL, accountID, datastreamID, 'DIGEST', 'CREATED')
endJob=update_job_status(apiToken, serverURL, accountID, ['653729699419320320','653726237407244288'], 'INGESTDATA', 'COMPLETED')
