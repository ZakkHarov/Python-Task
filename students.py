import json
from sqlite3 import OperationalError
import psycopg2
import sys
import argparse
import xml.etree.ElementTree as ET

from sympy import re

connection = psycopg2.connect(
    database='students', user='postgres', password='postgres', host='localhost', port='5432')
select_rooms_by_quantity = "SELECT r.name, COUNT(s.id) as Students_quantity FROM rooms r JOIN students s on s.room = r.id GROUP BY r.name order by r.name"
select_rooms_by_avg_age = "SELECT r.name, AVG(date_part('year',age(s.birthday))) Avg_age FROM rooms r JOIN students s on s.room = r.id GROUP BY r.name order by avg_age asc limit 5"
select_rooms_by_max_dif_age = "SELECT R.NAME, MAX(DATE_PART('year',AGE(S.BIRTHDAY)))-MIN(DATE_PART('year',AGE(S.BIRTHDAY))) as DIF_AGE FROM ROOMS R JOIN STUDENTS S ON S.ROOM = R.ID GROUP BY R.NAME ORDER BY DIF_AGE DESC LIMIT 5"
select_rooms_with_both_genders = "SELECT R.NAME, COUNT(CASE S.SEX WHEN 'M' THEN 1 ELSE NULL END) AS MEN_QUANTITY, COUNT(CASE S.SEX WHEN 'F' THEN 1 ELSE NULL END) AS WOMEN_QUANTITY FROM ROOMS R JOIN STUDENTS S ON S.ROOM = R.ID GROUP BY R.NAME HAVING COUNT(CASE S.SEX WHEN 'M' THEN 1 ELSE NULL END) > 0 AND COUNT(CASE S.SEX WHEN 'F' THEN 1 ELSE NULL END) > 0 ORDER BY R.NAME"
create_idx_rooms_id = "CREATE INDEX idx_rooms_id ON rooms(id)"
create_idx_rooms_name = "CREATE INDEX idx_rooms_name ON rooms(name)"
create_idx_students_room = "CREATE INDEX idx_students_room ON students(room)"
create_idx_students_sex = "CREATE INDEX idx_students_sex ON students(sex)"


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-rooms', default='rooms.json')
    parser.add_argument('-students', default='students.json')
    parser.add_argument('-format', default='xml')
    return parser


def main():
    parser = createParser()
    args = parser.parse_args(sys.argv[1:])
    file_rooms = open(args.rooms)
    file_students = open(args.students)
    format = args.format
    data_rooms = json.load(file_rooms)
    data_students = json.load(file_students)
    #insert_data(data_rooms, data_students, connection)
    #execute_index_query(connection, create_idx_rooms_id)
    #execute_index_query(connection, create_idx_rooms_name)
    #execute_index_query(connection, create_idx_students_room)
    #execute_index_query(connection, create_idx_students_sex)

    print("Rooms and student quantity:")
    print("Room           Quantity")
    for e_1, e_2 in execute_read_query(connection, select_rooms_by_quantity):
        print("{:<15}{:<10}".format(e_1, e_2))
    print("Top 5 rooms with lowest average age:")
    print("Room           Avg age")
    for e_1, e_2 in execute_read_query(connection, select_rooms_by_avg_age):
        print("{:<15}{:<10}".format(e_1, e_2))
    print("Top 5 rooms with max age difference:")
    print("Room           Dif age")
    for e_1, e_2 in execute_read_query(connection, select_rooms_by_max_dif_age):
        print("{:<15}{:<10}".format(e_1, e_2))
    print("Rooms both genders:")
    print("Room           Men       Women")
    for e_1, e_2, e_3 in execute_read_query(connection, select_rooms_with_both_genders):
        print("{:<15}{:<10}{}".format(e_1, e_2, e_3))

    if(format == "json"):
        result = {}
        result['rooms_by_quantity'] = []
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_quantity):
            result['rooms_by_quantity'].append({
                'room': e_1,
                'quantity': e_2
            })
        result['rooms_by_avg_age'] = []
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_avg_age):
            result['rooms_by_avg_age'].append({
                'room': e_1,
                'avg_age': e_2
            })
        result['rooms_by_max_dif_age'] = []
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_max_dif_age):
            result['rooms_by_max_dif_age'].append({
                'room': e_1,
                'dif_age': e_2
            })
        result['rooms_with_both_genders'] = []
        for e_1, e_2, e_3 in execute_read_query(connection, select_rooms_with_both_genders):
            result['rooms_with_both_genders'].append({
                'room': e_1,
                'men': e_2,
                'women': e_3
            })
        with open('result.json', 'w') as outfile:
            json.dump(result, outfile)
    elif(format == "xml"):
        data = ET.Element('data')
        rooms_by_quantity = ET.SubElement(data, 'rooms_by_quantity')
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_quantity):
            item = ET.SubElement(rooms_by_quantity, 'item')
            room = ET.SubElement(item, 'room')
            room.text = e_1
            quantity = ET.SubElement(room, 'quantity')
            quantity.text = str(e_2)
        rooms_by_avg_age = ET.SubElement(data, 'rooms_by_avg_age')
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_avg_age):
            item = ET.SubElement(rooms_by_avg_age, 'item')
            room = ET.SubElement(item, 'room')
            room.text = e_1
            avg_age = ET.SubElement(room, 'avg_age')
            avg_age.text = str(e_2)
        rooms_by_max_dif_age = ET.SubElement(data, 'rooms_by_max_dif_age')
        for e_1, e_2 in execute_read_query(connection, select_rooms_by_max_dif_age):
            item = ET.SubElement(rooms_by_max_dif_age, 'item')
            room = ET.SubElement(item, 'room')
            room.text = e_1
            max_dif_age = ET.SubElement(room, 'max_dif_age')
            max_dif_age.text = str(e_2)
        rooms_with_both_genders = ET.SubElement(
            data, 'rooms_with_both_genders')
        for e_1, e_2, e_3 in execute_read_query(connection, select_rooms_with_both_genders):
            item = ET.SubElement(rooms_with_both_genders, 'item')
            room = ET.SubElement(item, 'room')
            room.text = e_1
            men = ET.SubElement(room, 'men')
            men.text = str(e_2)
            women = ET.SubElement(room, 'women')
            women.text = str(e_3)
        mydata = ET.ElementTree(data)

        mydata.write("result.xml")
    else:
        print("Unknown format")

    file_rooms.close
    file_students.close


def insert_data(rooms, students, connection):
    insert_data_rooms(rooms, connection)
    insert_data_students(students, connection)
    print("Data from json files inserted!")


def insert_data_rooms(rooms, connection):
    try:
        with connection.cursor() as cursor:
            for i in rooms:
                sql = "INSERT INTO rooms (id,name) VALUES (%s,%s)"
                cursor.execute(sql, (i["id"], i["name"]))
        connection.commit()
    finally:
        connection.close()


def insert_data_students(students, connection):
    try:
        with connection.cursor() as cursor:
            for i in students:
                sql = "INSERT INTO students (birthday,id,name,room,sex) VALUES (%s,%s,%s,%s,%s)"
                cursor.execute(
                    sql, (i["birthday"], i["id"], i["name"], i["room"], i["sex"]))
        connection.commit()
    finally:
        connection.close()


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_index_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Index inserted!")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


main()
