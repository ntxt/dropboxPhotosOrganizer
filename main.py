import dropbox
from datetime import datetime
from time import sleep
import sys
import re

# returns a pair of paths - source and destination. The destination is based
# on the name of the source path if it matches the format YYYY-MM-DD, otherwise on the
# "client_modified" property, which is when it was uploaded to Dropbox first.
# The destination path goes along "destFolder/YYYY/MM-DD/original-file-name" format

def create_new_path(entry, destFolder):
    #print entry
    fnameSrc = entry.name #.encode('utf-8')
    fnameDest = "{0}/{1:04d}/{2:02d}-{3:02d}/{4}"
    m = re.search("^(\d{4})-(\d\d)-(\d\d)", fnameSrc)
    if(m != None):
        year, month, day = map( lambda x: int(x), m.group(1,2,3) )
    else:
        year, month, day = entry.client_modified.timetuple()[:3]
    
    return [
            entry.path_lower, 
            fnameDest.format(destFolder, year, month, day, fnameSrc)
        ]

def unique_items( batch ):
    index = {}
    unique = []
    for item in batch:
        path = item.to_path.lower()
        newpath = item.to_path
        if(path in index):
            occurenceNo = index[path]
            newpath = re.sub(
                r"\.([^.]+)$", 
                "-%s.\\1" % occurenceNo, 
                item.to_path
            )
            index[path] += 1
        else:
            index[path] = 1
        unique.append(dropbox.files.RelocationPath(item.from_path, newpath))
    return unique


# --------------------------------------------------
# Take the API token from the cmd arguments and start
if( len(sys.argv) != 4 ):
    print("USAGE: " + sys.argv[0] + " API_TOKEN '/source/folder' 'destination/folder'")
    sys.exit(1)
token = sys.argv[1]
srcFolder = sys.argv[2]
destFolder = sys.argv[3]

print("About to move files from folder {0} to {1}.".format(srcFolder, destFolder))
maxItemsPerBatch = 100
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
dbx = dropbox.Dropbox(token)
print (dbx.users_get_current_account())
print
# First collect all items (recursively) from the source folder,
# following the cursor as long as there's anything left

imgList = dbx.files_list_folder(srcFolder, True)
batchCount = 0
batches = []
index = 0

while True:
    for entry in imgList.entries:

        # skip folder entries
        try:
            entry.client_modified
        except AttributeError:
            ".. folder " + entry.name
            continue

        # open a new batch for every 'maxItemsPerBatch' items
        if(index % maxItemsPerBatch == 0):
            batches.append( [] )
            batchCount += 1  

        fnameSrc, fnameDest = create_new_path(entry, destFolder)
        relocation = dropbox.files.RelocationPath(fnameSrc, fnameDest)
        batches[batchCount-1].append( relocation )
        logLine = fnameSrc + " -> " + fnameDest + "\n"
        #batchFile.write( logLine )
        index += 1
    
    if( not imgList.has_more ):
        break
    print ("{0} files found, fetching next chunk".format(index))
    imgList = dbx.files_list_folder_continue( imgList.cursor )

if(len(batches) == 0):
    print ("Nothing to do apparently")
    exit(0)

# batches ready to be moved, now we need to do it sequentially to avoid
# "too many write operations" error from Dropbox    

print("Sending batches to move, {0} files each".format(maxItemsPerBatch))
results = []
for i, batch in enumerate(batches):
    batch = unique_items(batch)
    job = dbx.files_move_batch( batch, True, True, True )
    id = job.get_async_job_id()
    if (not job.is_complete()):
        while True:
            print ("checking job ", id)
            try:
                result = dbx.files_move_batch_check(id)
            except dropbox.exceptions.ApiError as err:
                print("API error: ", err)
                for item in batch: print(item)
                results.append( "failed" )
                break               
            if( result.is_complete() ):
                results.append( "complete" )
                break
            elif( result.is_failed() ):
                print(result,"\n\n")
                for item in batch: print(item)
                results.append( "failed" )
                break
            else:
                # wait a little before polling again. In my case it takes abt. 40 secs per batch
                print ("and again in a few secs...")
                sleep(15)
    print ("job " + id + " finished", "{0} of {1}".format(i, len(batches)))
print (results)



