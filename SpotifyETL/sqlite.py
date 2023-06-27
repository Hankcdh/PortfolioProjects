import sqlite3


# Create to database
conn = sqlite3.connect('çustomer.db')


#Create a cursor 
c = conn.cursor()

#Create a Table

many_customers = [
    ('Ted','Brown','ted@brown.com'),
    ('Steph','Kuewa','step@kuewa.com'),
    ('Dan','Pas','dan@pas.com'),
]

c.execute("""
     CREATE TABLE IF NOT EXISTS Customers (
    first_name text,
    last_name text,
    email_address text
    )""")


c.execute("INSERT INTO Customers VALUES ('Ted','Brown','ted@brown.com')" )

c.execute("INSERT INTO Customers VALUES ('Ted','Brown','ted@brown.com')" )
print("Insert Completed ")
c.execute("SELECT * FROM Customers" )
print(c.fetchall())
print("Select Completed")

# NUll
# INTEGER
# REAL
# TEXT 
# BLOB

#Commit our command
conn.commit()
conn.close()

