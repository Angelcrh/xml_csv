

import codecs
import os
import xml.etree.ElementTree as et
import pandas as pd
import mysql.connector
import sqlite3

import mysql


class Xml_csv:

    def __init__(self, input_file, output_file, encoding='utf-8'):

        # Initialize the class with the paths to the input xml file
        # and the output csv file

        self.output_buffer = []
        self.output = None

        # open the xml file for iteration
        self.context = et.iterparse(input_file, events=("start", "end"))

        # output file handle
        try:
            self.output = codecs.open(output_file, "w", encoding=encoding)
        except:
            print("Failed to open the output file")
            raise

    def convert(self, tag="item", delimiter=",", ignore=[], noheader=False,
                limit=-1, buffer_size=1000, quotes=True):

        # Convert the XML file to CSV file

        # get to the root
        try:
            try:
                # for py version 2.x
                event, root = self.context.next()
            except AttributeError:
                # for py version 3.x
                event, root = next(self.context)
        except et.ParseError as e:
            # Invalid XML file - so close the file handle and delete it
            self.output.close()
            os.remove(self.input_file)
            raise e

        items = []
        header_line = []
        field_name = ''
        processed_fields = []

        tagged = False
        started = False
        n = 0

        # iterate through the xml
        for event, elem in self.context:
            # if elem is an unignored child node of the record tag, it should be written to buffer
            should_write = elem.tag != tag and started and elem.tag not in ignore
            # and if a header is required and if there isn't one
            should_tag = not tagged and should_write and not noheader

            if event == 'start':
                if elem.tag == tag:
                    processed_fields = []
                if elem.tag == tag and not started:
                    started = True
                elif should_tag:
                    # if elem is nested inside a "parent", field name becomes parent_elem
                    field_name = '_'.join((field_name, elem.tag)) if field_name else elem.tag

            else:
                if should_write and elem.tag not in processed_fields:
                    processed_fields.append(elem.tag)
                    if should_tag:
                        header_line.append(field_name)  # add field name to csv header
                        # remove current tag from the tag name chain
                        field_name = field_name.rpartition('_' + elem.tag)[0]
                    items.append('' if elem.text is None else elem.text.strip().replace('"', r'""'))

                # end of traversing the record tag
                elif elem.tag == tag and len(items) > 0:
                    # csv header (element tag names)
                    if header_line and not tagged:
                        self.output.write(delimiter.join(header_line) + '\n')
                    tagged = True

                    # send the csv to buffer
                    if quotes:
                        self.output_buffer.append(r'"' + (r'"' + delimiter + r'"').join(items) + r'"')
                    else:
                        self.output_buffer.append((delimiter).join(items))
                    items = []
                    n += 1

                    # halt if the specified limit has been hit
                    if n == limit:
                        break

                    # flush buffer to disk
                    if len(self.output_buffer) > buffer_size:
                        self._write_buffer()

                elem.clear()  # discard element and recover memory

        self._write_buffer()  # write rest of the buffer to file
        self.output.close()

        return n

    def _write_buffer(self):
        # Write records from buffer to the output file

        self.output.write('\n'.join(self.output_buffer) + '\n')
        self.output_buffer = []


def findXmlFiles(path, listOfFiles):
    for filename in os.listdir(path):
        if not filename.endswith('.xml'): continue
        fullname = os.path.join(path, filename)
        listOfFiles.append(fullname)


def uploadDtb(sample):
    # Import CSV
    data = pd.read_csv(sample)
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


def main():
    listOffiles = []
    findXmlFiles("F:/xml_python/", listOffiles)
    number = 1
    for file in listOffiles:
        converter = Xml_csv(file, "F:/xml_python/sample{}.csv".format(number), "utf-8")
        converter.convert("employee")
        uploadDtb("F:/xml_python/sample{}.csv".format(number))
        number = number + 1




if __name__ == "__main__":
    main()
