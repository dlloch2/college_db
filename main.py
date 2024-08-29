import pandas as pd
import sqlite3

ux_inst = pd.read_csv('data/UX_INST.csv')
ug_admis = pd.read_csv('data/UG_ADMIS.csv')
ug_enroll = pd.read_csv('data/UG_ENROLL.csv')
ug_entr_exams = pd.read_csv('data/UG_ENTR_EXAMS.csv') 


institution_info = ux_inst[['INUN_ID', 'NAME', 'CITY', 'STATE_CODE', 'ZIPCODE']]

# admission info
ug_admis_filtered = ug_admis[['INUN_ID', 'AP_RECD_1ST_N', 'AP_ADMT_1ST_N', 'AP_DL_FRSH_MON', 'AP_DL_FRSH_DAY', 'AP_NOTF_DL_FRSH_MON', 'AP_NOTF_DL_FRSH_DAY']].copy()

ug_admis_filtered.loc[:, 'ACCEPTANCE_RATE'] = (ug_admis_filtered['AP_ADMT_1ST_N'] / ug_admis_filtered['AP_RECD_1ST_N']) * 100

ug_enroll_filtered = ug_enroll[['INUN_ID', 'EN_TOT_UG_N']]

merged_data = pd.merge(institution_info, ug_admis_filtered, on='INUN_ID')
merged_data = pd.merge(merged_data, ug_enroll_filtered, on='INUN_ID')

# print(merged_data.head())

# AP_RECD_1ST_N: Number of first-time, first-year (freshman) applicants received by the institution.
# AP_ADMT_1ST_N: Number of first-time, first-year (freshman) applicants admitted by the institution.
# AP_DL_FRSH_MON: The month by which applications for first-time, first-year students are due.
# AP_DL_FRSH_DAY: The day of the month by which applications for first-time, first-year students are due.
# AP_NOTF_DL_FRSH_MON: The month by which the institution notifies first-time, first-year students of their admission status.
# AP_NOTF_DL_FRSH_DAY: The day of the month by which the institution notifies first-time, first-year students of their admission status.
# ACCEPTANCE_RATE (calculated): The percentage of applicants who were admitted, calculated as (AP_ADMT_1ST_N / AP_RECD_1ST_N) * 100.
# INUN_ID: Institution ID
# EN_TOT_UG_N: The total number of undergraduate students enrolled at the institution. 

# used to create the DB
conn = sqlite3.connect('college_info.db')
cursor = conn.cursor()

# this table is where u store all the data
cursor.execute('''
CREATE TABLE IF NOT EXISTS colleges (
    INUN_ID INTEGER PRIMARY KEY,
    NAME TEXT,
    CITY TEXT,
    STATE_CODE TEXT,
    ZIPCODE TEXT,
    ACCEPTANCE_RATE REAL,
    APP_DEADLINE_MONTH INTEGER,
    APP_DEADLINE_DAY INTEGER,
    NOTIFY_DEADLINE_MONTH INTEGER,
    NOTIFY_DEADLINE_DAY INTEGER,
    STUDENT_BODY_SIZE INTEGER
)
''')

for _, row in merged_data.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO colleges (INUN_ID, NAME, CITY, STATE_CODE, ZIPCODE, ACCEPTANCE_RATE, 
                                     APP_DEADLINE_MONTH, APP_DEADLINE_DAY, NOTIFY_DEADLINE_MONTH, 
                                     NOTIFY_DEADLINE_DAY, STUDENT_BODY_SIZE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        row['INUN_ID'],
        row['NAME'],
        row['CITY'],
        row['STATE_CODE'],
        row['ZIPCODE'],
        row['ACCEPTANCE_RATE'],
        row['AP_DL_FRSH_MON'],
        row['AP_DL_FRSH_DAY'],
        row['AP_NOTF_DL_FRSH_MON'],
        row['AP_NOTF_DL_FRSH_DAY'],
        row['EN_TOT_UG_N']
    ))

conn.commit()
conn.close()

print("Data successfully transferred to SQLite database!")


# how to connect to DB
conn = sqlite3.connect('college_info.db')
cursor = conn.cursor()

cursor.execute("SELECT * FROM colleges LIMIT 30")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
