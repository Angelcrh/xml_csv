from turtle import pd

import mysql


class People:

    def uploadDtb(self):
        # Import CSV
        data = pd.read_csv(self)
        df = pd.DataFrame(data, columns=['name', 'phone', 'email', 'date', 'country'])

        print(df)

        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="xml_csv"
        )
        if mydb.is_connected():
            print("Connexion correcta")

        cursor = mydb.cursor()

        # # # # Create Table
        # exist = cursor.execute(''' SELECT table_name FROM information_schema.tables WHERE table_schema = 'xml_csv' ''')
        # # if the count is 1, then table exists
        # if cursor.rowcount:
        #     print('Table exists.')
        #
        # else:
        #     cursor.execute('CREATE TABLE people_info (Name nvarchar(50), Phone int, Email nvarchar(50), Date date, '
        #                    'Country nvarchar(50))')

        # # Insert DataFrame to Table
        for row in df.itertuples():
            values = (row.name, row.phone, row.email, row.date, row.country)
            cursor.execute('''
                        INSERT INTO people_info (name, phone, email, date , country)
                        VALUES (%s,%s,%s,%s,%s)
                        ''',
                           values
                           )
        mydb.commit()