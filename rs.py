import psycopg2
conn = psycopg2.connect(
    host="redshift-cluster-1.cn8ui4uqu7fc.us-west-1.redshift.amazonaws.com",
    database="dev",
    user="admin",
    port=5439,
    password="Admin123")
cur = conn.cursor()
with open('emp.sql') as f:
    cursor.execute(f.read().decode('utf-8'), multi=True)
#sqlfile=open("emp.sql","r")
#cur.execute("sqlfile")
print("connection succussfully established")
cur.execute("select * from users")
print(cur.fetchall())
print(".........")
print()
