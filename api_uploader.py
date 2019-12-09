import os, requests
import json, time
from itertools import islice
from manage_jobs import create_job, get_jobs, update_job_status

###### Job Variables
accountID = 648687965271797760
datastreamID=653532737151868928
serverURL="https://staging.falkonry.ai"
apiToken="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4OTEwMzU4NTcsICJlbWFpbCIgOiAid2FycmVuLmtpbUBmYWxrb25yeS5jb20iLCAibmFtZSIgOiAid2FycmVuLmtpbUBmYWxrb25yeS5jb20iLCAic2Vzc2lvbiIgOiAiNjUyNjU2NTkyMTczNjA4OTYwIiB9.uSRCgbVByFjyJtebYkoHl2_xNf77bzeTywFQaJ-yFl0"
entityCol = "ENTITY"
timeFormat="YYYY-M-DD H:m:s.SS"
timeZone="America/Los_Angeles"
timeIdentifier="time"
r = {}

###### Automatic variables
input_file_directory="files-to-upload"
i=0
chunksize=10000
filesize=5000000
filesizemax=filesize-1
injectEntity=False

for input_file in os.listdir("./"+input_file_directory):
  print("Processing "+str(input_file))
  with open("./"+input_file_directory+'/'+input_file) as f:
    ###### Determine optimal chunksize for specified filesize
    print("Determining optimal number of lines for ~5MB chunksize...")
    while filesize > filesizemax:
      with open("./"+input_file_directory+'/'+input_file) as f:
        head = list(islice(f, chunksize))
        testchunk = ''.join(head)
        testfile = open("checksize", "w")
        testfile.write(testchunk)
        testfile.close()
        filesize=os.path.getsize('checksize')
        if filesize > (filesizemax-1)/1000:
          chunksize=chunksize-100
      os.remove("checksize")
    print("Using chunksize=" + str(chunksize))
    
    ##### Get CSV header
    with open("./"+input_file_directory+'/'+input_file) as f:
      header = f.readline()
      if entityCol not in header:
        header = header.strip('\n').strip('\r')+','+entityCol+'\n'
        injectEntity=True
    
    ##### Process upload in chunks
    with open("./"+input_file_directory+'/'+input_file) as f:
      while True:
        next_n_lines = list(islice(f, chunksize))
        if not next_n_lines:
          break
          
        # Check for pending DIGEST jobs
        runningDigestJobs=get_jobs(apiToken, serverURL, accountID, datastreamID, 'DIGEST', 'CREATED')
        while runningDigestJobs:
          print("DIGEST jobs in pending status were found:")
          print(runningDigestJobs)
          print("Sleeping for 20 seconds before attempting resume...")
          time.sleep(20)
          runningDigestJobs=get_jobs(apiToken, serverURL, accountID, datastreamID, 'DIGEST', 'CREATED')
    
        # Create INGEST endpoint
        resp=create_job(apiToken, serverURL, accountID, datastreamID, 'INGESTDATA', entityCol, timeIdentifier, timeFormat, timeZone)
        
        # Start INGEST job
        lines = [x.strip('\n').strip('\r') for x in next_n_lines]
        if injectEntity is True:
          payload = (','+entityCol+'\n').join(lines)+','+entityCol+'\n'
        else:
          payload = '\n'.join(lines)
        if i>0:
          payload=header+payload
        authTokenCSV = {"content-type":"text/csv", "Authorization" : "Bearer "+apiToken}
        r = requests.post(serverURL + resp['url'], data=payload, headers=authTokenCSV)
        print("Chunk "+str(i)+" has been uploaded.")
        i=i+1
          
        # End INGEST job
        endJob=update_job_status(apiToken, serverURL, accountID, [resp['linkid']], 'INGESTDATA', 'COMPLETED')