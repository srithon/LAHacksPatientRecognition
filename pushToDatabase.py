import sqlite3
import csv

# con = sqlite3.connect(":memory:")
con = sqlite3.connect('patientsDB.db')
cur = con.cursor()
cur.execute("CREATE TABLE t (GroupID,PatientID,PatientAcctNum,FirstName,MI,LastName,DateofBirth,Sex,CurrentStreetA,CurrentStreetB,CurrentCity,CurrentState,CurrentZipCode,PreviousFirstName,PreviousMI,PreviousLastName,PreviousStreetA,PreviousStreetB,PreviousCity,PreviousState,PreviousZipCode);") # use your column names here

with open('Patient Matching Data.csv','r') as fin: # `with` statement available in B.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['GroupID'], i['PatientID'], i['PatientAcctNum'],i['FirstName'],i['MI'],i['LastName'],i['DateofBirth'],i['Sex'],i['CurrentStreetA'],i['CurrentStreetB'],i['CurrentCity'],i['CurrentState'],i['CurrentZipCode'],i['PreviousFirstName'],i['PreviousMI'],i['PreviousLastName'],i['PreviousStreetA'],i['PreviousStreetB'],i['PreviousCity'],i['PreviousState'],i['PreviousZipCode']) for i in dr]

cur.executemany("INSERT INTO t (GroupID,PatientID,PatientAcctNum,FirstName,MI,LastName,DateofBirth,Sex,CurrentStreetA,CurrentStreetB,CurrentCity,CurrentState,CurrentZipCode,PreviousFirstName,PreviousMI,PreviousLastName,PreviousStreetA,PreviousStreetB,PreviousCity,PreviousState,PreviousZipCode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?);", to_db)
con.commit()
con.close()