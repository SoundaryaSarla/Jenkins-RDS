import psycopg2
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
s3.Bucket('redshiftdatajenkins/load').download_file(key,local_file_name)
#print(os.system('ls -ltrh'))
with open('/tmp/test.csv', 'r') as f:
# Notice that we don't need the `csv` module
    next(f) # Skip the header row.
    #cursor.execute("SET search_path = geagp_cdoo_healthcheck, public;")
    cursor.copy_from(f, 'hadoop_emr_ldp_nonprod_availability', sep=',')
#cursor.execute("\copy geagp_cdoo_healthcheck.hadoop_ldp_db_availability_report_test_event from '/tmp/test.csv' with delimiter ',' ;")
cursor.execute("select count(*) from hadoop_emr_ldp_nonprod_availability;")
print("table count after load")
myresult = cursor.fetchall()
for x in myresult:
  print(x)
conn.commit()
cursor.close()
print("working")
conn.close()
