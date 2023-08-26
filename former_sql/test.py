import sqlite3
import os
import pandas as pd
from typing import List
import time


# pd.set_option('display.colheader_justify', 'right')
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)


def printAns(question_title: str, columns: List[str], data: List):
    print()
    print("---")
    print(question_title)
    results = pd.DataFrame.from_records(data=data, columns=columns)
    print(results.to_string(index=False))


def printCursorExecuteDebug(query):
    # print()
    # print("---")
    cols = [column[0] for column in query.description]
    results = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    with open(
        f"./debug-{time.strftime('%y%m%d-%H%M%S.txt')}", "w", encoding="utf-8"
    ) as fd:
        print(results.to_string(index=False), file=fd)


os.remove("db.sqlite")
conn = sqlite3.connect("db.sqlite")
cursor = conn.cursor()
with open("course_data_1nf_2023.sql", encoding="utf-8") as f:
    cursor.executescript(f.read())

# ---
# create table
# ---

# create "location" table
#   location_id         PK
#   room
#   building
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "location" (
        "location_id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "room" varchar(20),
        "building" varchar(20)
    )
"""
)

# create "course" table
#   semester            PK
#   course_no           PK
#   course_name
#   course_type
#   course_credit
#   course_limit
#   course_status
#   course_location     FK
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "course" (
        "semester" varchar(4),
        "course_no" INTEGER,
        "course_name" varchar(255),
        "course_type" varchar(10),
        "course_credit" INTEGER,
        "course_limit" INTEGER,
        "course_status" varchar(10),
        "course_location" INTEGER,
        PRIMARY KEY (semester, course_no),
        FOREIGN KEY (course_location) REFERENCES location (location_id)
    )
"""
)

# create "curriculum_field" table
#   semester            PK FK
#   course_no           PK FK
#   curriculum_field    PK
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "curriculum_field" (
        semester varchar(4),
        course_no INTEGER,
        curriculum_field TEXT,
        PRIMARY KEY (semester, course_no, curriculum_field),
        FOREIGN KEY (semester, course_no) REFERENCES course (semester, course_no)
    )
"""
)

# create "course_time" table
#   semester            PK FK
#   course_no           PK FK
#   time_slot           PK
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "course_time" (
        semester varchar(4),
        course_no INTEGER,
        time_slot varchar(20),
        PRIMARY KEY (semester, course_no, time_slot),
        FOREIGN KEY (semester, course_no) REFERENCES course (semester, course_no)
    )
"""
)

# create "student" table
#   student_id          PK
#   student_name
#   student_dept
#   student_grade
#   student_status
#   student_class
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "student" (
        "student_id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "student_name" varchar(20),
        "student_dept" varchar(30),
        "student_grade" INTEGER,
        "student_status" varchar(10),
        "student_class" varchar(1)
    )
"""
)

# create "teacher" table
#   teacher_id          PK
#   teacher_name
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "teacher" (
        "teacher_id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "teacher_name" varchar(20)
    )
"""
)

# create "teach" table
#   semester            PK FK
#   course_no           PK FK
#   teacher_id          PK    FK
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "teach" (
        "semester" varchar(4),
        "course_no" INTEGER,
        "teacher_id" INTEGER,
        PRIMARY KEY (semester, course_no, teacher_id),
        FOREIGN KEY (semester, course_no) REFERENCES course (semester, course_no),
        FOREIGN KEY (teacher_id) REFERENCES teacher (teacher_id)
    )
"""
)

# create "enroll" table
#   semester            PK FK
#   course_no           PK FK
#   student_id          PK    FK
#   select_result
#   course_score
#   feedback_rank
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS "enroll" (
        "semester" varchar(4),
        "course_no" INTEGER,
        "student_id" INTEGER,
        "select_result" varchar(10),
        "course_score" NUMERIC,
        "feedback_rank" INTEGER,
        PRIMARY KEY (semester, course_no, student_id),
        FOREIGN KEY (semester, course_no) REFERENCES course (semester, course_no),
        FOREIGN KEY (student_id) REFERENCES student (student_id)
    )
"""
)

# ---
# insert data
# ---

cursor.execute(
    """
    INSERT INTO
        "location" (room, building)
    SELECT DISTINCT
        course_room,
        course_building
    FROM
        course_data;
"""
)

cursor.execute(
    """
    INSERT INTO
        "course"
    SELECT DISTINCT
        semester,
        course_no,
        course_name,
        course_type,
        course_credit,
        course_limit,
        course_status,
        location_id
    FROM
        course_data JOIN location ON course_room = room AND course_building = building;
"""
)

cursor.execute("SELECT semester, course_no FROM course")
all_courses = list(cursor.fetchall())

# populate curriculum_field
# curriculum_field is comma separated
for semester, course_no in all_courses:
    cursor.execute(
        "SELECT curriculum_field FROM course_data WHERE semester=? AND course_no=?",
        (semester, course_no),
    )
    curriculum_field = cursor.fetchone()[0]
    for field in curriculum_field.split(","):
        cursor.execute(
            "INSERT INTO curriculum_field VALUES (?, ?, ?)",
            (semester, course_no, field),
        )


# populate course_time
# course_time is comma separated
for semester, course_no in all_courses:
    cursor.execute(
        "SELECT course_time FROM course_data WHERE semester=? AND course_no=?",
        (semester, course_no),
    )
    course_time = cursor.fetchone()[0]
    for time_slot in course_time.split(","):
        cursor.execute(
            "INSERT INTO course_time VALUES (?, ?, ?)", (semester, course_no, time_slot)
        )

# populate student
cursor.execute(
    """
    WITH "distinct_studedent" AS (
        SELECT DISTINCT
            student_name,
            student_dept,
            student_grade,
            student_status,
            student_class
        FROM
            course_data
    )
    INSERT INTO
	    "student" (student_name, student_dept, student_grade, student_status, student_class)
    SELECT
        student_name,
        student_dept,
        student_grade,
        student_status,
        student_class
    FROM
        distinct_studedent;
"""
)

# populate teacher and teach
# teacher is comma separated
for semester, course_no in all_courses:
    cursor.execute(
        "SELECT teacher_name FROM course_data WHERE semester=? AND course_no=?",
        (semester, course_no),
    )
    teacher_name_multi = cursor.fetchone()[0]
    for teacher_name in teacher_name_multi.split(","):
        cursor.execute(
            "INSERT INTO teacher (teacher_name) VALUES (?)",
            (teacher_name,),
        )
        cursor.execute(
            "INSERT INTO teach VALUES (?, ?, last_insert_rowid())",
            (semester, course_no),
        )

# populate enroll
cursor.execute(
    """
    INSERT INTO
        "enroll"
    SELECT
        semester,
        course_no,
        student_id,
        select_result,
        course_score,
        feedback_rank
    FROM
        course_data c
        join student s on c.student_name = s.student_name;
"""
)

cursor.execute("DROP TABLE course_data")

# ---
# ans zone
# ---

question_title = "3.1 「A0001微積分」上課地點要由K205修改到K210 大教室，該怎麼做？"
# cursor.execute(
#     """
#     UPDATE
#         course
#     SET
#         course_room = 'K210'
#     WHERE
#         course_no = 'A0001' and course_room = 'K205';
# """
# )
cursor.executescript(
    """
    INSERT INTO location (room, building) VALUES ('K210', '工程一館');
    UPDATE
        course
    SET
        course_location = last_insert_rowid()
    WHERE
        course_no = 'A0001';
"""
)


question_title = "3.2 請列出「A0002 計算機概」的修課名單（點名表）"
cols = ["姓名", "系所", "年級", "班級"]
cursor.execute(
    """
    SELECT
        student_name, student_dept, student_grade, student_class
    FROM
        course
        JOIN enroll USING (semester, course_no)
        JOIN student USING (student_id)
    WHERE
        course_no = 'A0002' and select_result = '中選';
"""
)
printAns(
    question_title=question_title,
    columns=cols,
    data=cursor.fetchall(),
)


question_title = "3.3 請列出課程成績不及格的學生比例資料（大學部：低於60分、碩博：70分）"
cols = ["課名", "授課教師", "不及格人次", "修課人次", "不及格比例"]
cursor.execute(
    """
    WITH undergraduates AS (
        SELECT student_id FROM student WHERE student_dept LIKE '%系'
    ), graduates AS (
        SELECT student_id FROM student WHERE student_dept NOT LIKE '%系'
    ), course_participation AS (
        SELECT * FROM enroll WHERE select_result = '中選'
    ), student_count AS (
        SELECT semester, course_no, COUNT(*) AS student_count
        FROM course_participation
        GROUP BY semester, course_no
    ), fail_threshold AS (
        SELECT semester, course_no, student_id, (
            CASE
                WHEN student_id IN undergraduates THEN 60
                WHEN student_id IN graduates THEN 70
            END
        ) AS threshold
        FROM course_participation
    ), result_tbl AS (
        SELECT semester, course_no, student_count, COUNT(*) AS fail_count
        FROM enroll
            JOIN fail_threshold USING (semester, course_no, student_id)
            JOIN student_count USING (semester, course_no)
        WHERE course_score < threshold
        GROUP BY semester, course_no
    )
    SELECT course_name, teacher_name, fail_count, student_count, (fail_count * 1.0 / student_count) AS fail_rate
    FROM result_tbl
        JOIN course USING (semester, course_no)
        JOIN teach USING (semester, course_no)
        JOIN teacher USING (teacher_id)
"""
)
printAns(
    question_title=question_title,
    columns=cols,
    data=cursor.fetchall(),
)


question_title = "3.4 請列出各系學生修課領域分佈情況"
cols = ["學生系所", "課程領域", "人次", "佔比"]
# query = cursor.execute(
cursor.execute(
"""
WITH dept_student_view AS (
    SELECT student_dept, COUNT(*) AS dept_persons
    FROM enroll
        JOIN student USING (student_id)
    WHERE select_result = '中選'
    GROUP BY student_dept
)

SELECT student_dept, cf.curriculum_field, COUNT(*) AS field_persons, (COUNT(*) *1.0 / dept_persons) AS field_rate
FROM curriculum_field AS cf
    JOIN enroll USING (semester, course_no)
    JOIN student USING (student_iD)
    JOIN dept_student_view USING (student_dept)
WHERE select_result = '中選'
GROUP BY student_dept, cf.curriculum_field
"""
)
# printCursorExecuteDebug(query)
printAns(
    question_title=question_title,
    columns=cols,
    data=cursor.fetchall(),
)


question_title = "3.5 請列出教學評量平均分數及總分"
cols = ["課名", "授課教師", "教學評量總分", "教學評量平均分數"]
query = cursor.execute(
"""
SELECT course_name, teacher_name, SUM(feedback_rank), AVG(feedback_rank)
FROM course
    JOIN enroll USING (semester, course_no)
    JOIN teach USING (semester, course_no)
    JOIN teacher USING (teacher_id)
WHERE feedback_rank IS NOT NULL
GROUP BY course_name, teacher_name
"""
)

printCursorExecuteDebug(query)
# printAns(question_title=question_title,
#          columns=cols,
#          data=cursor.fetchall(),
# )


conn.commit()
conn.close()
