import psycopg2
import boto3
s3 = boto3.resource('s3')
key = 'load/part-csv.tbl-000'
conn = psycopg2.connect(
    host="redshift-jenkins.cpiazh88ds78.us-east-1.redshift.amazonaws.com",
    database="dev",
    user="awsuser",
    port=5439,
    password="Admin12345")
cur = conn.cursor()
print("connection succussfully established")
cur.execute("select city from users")
print(cur.fetchall())
print(".........")
print()
# download s3 csv file to lambda tmp folder
local_file_name = '/tmp/test.csv' #
print(local_file_name)
s3.Bucket('redshiftdatajenkins').download_file(key,local_file_name)
#print(os.system('ls -ltrh'))
with open('/tmp/test.csv', 'r') as f:
# Notice that we don't need the `csv` module
    next(f) # Skip the header row.
    #cursor.execute("SET search_path = geagp_cdoo_healthcheck, public;")
    cur.execute("SET search_path = public;")
    cur.copy_from(f, 'part', sep=',')
#cursor.execute("\copy dev.part from '/tmp/test.csv' with delimiter ',' ;")
cur.execute("select count(*) from dev.part;")
print("table count after load")
myresult = cursor.fetchall()
for x in myresult:
  print(x)
conn.commit()
cursor.close()
print("working")
conn.close()
