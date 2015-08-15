

key =bucket.get_key('/2.5_week.csv')
key.get_contents_to_filename('C:/Users/SONY/Desktop/Summer 2015/PA4/myfile.csv')
print " Completed_1"
from boto.s3.key import Key
k = Key(bucket)
k.key = 'firstfile.csv'
k.set_contents_from_filename('C:/Users/SONY/Desktop/Summer 2015/PA4/myfile.csv')
k.get_contents_to_filename('bar.jpg')
print " Completed_2"

# Get file info
source_path = 'C:/Users/SONY/Desktop/Summer 2015/PA4/myfile.csv'
source_size = os.stat(source_path).st_size

# Create a multipart upload request
mp = bucket.initiate_multipart_upload(os.path.basename(source_path))

# Use a chunk size of 50 MiB (feel free to change this)
chunk_size = 52428800
chunk_count = int(math.ceil(source_size / float(chunk_size)))

# Send the file parts, using FileChunkIO to create a file-like object
# that points to a certain byte range within the original file. We
# set bytes to never exceed the original file size.
for i in range(chunk_count):
    offset = chunk_size * i
    bytes = min(chunk_size, source_size - offset)
    with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
        mp.upload_part_from_file(fp, part_num=i + 1)

# Finish the upload
mp.complete_upload()
print "completed"

def dowload():
    key =bucket.get_key('/2.5_week.csv')
    key.get_contents_to_filename('C:/Users/SONY/Desktop/Summer 2015/PA4/myfile.csv')
    print " Completed"