import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="phonebook_db",
    user="postgres",
    password="Botashka07"
)

def execute_query(query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            print("Query executed successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error executing query:", error)

# 1. Кесте жасау
create_table_query = '''
CREATE TABLE IF NOT EXISTS phonebook2 (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255),
    phone_number VARCHAR(20) NOT NULL
);
'''

# 2. Pattern арқылы іздеу функциясы
find_by_pattern_function = '''
CREATE OR REPLACE FUNCTION find_by_pattern(pattern TEXT)
RETURNS TABLE(user_id INT, first_name VARCHAR, last_name VARCHAR, phone_number VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT user_id, first_name, last_name, phone_number
    FROM phonebook2
    WHERE first_name ILIKE '%' || pattern || '%'
       OR last_name ILIKE '%' || pattern || '%'
       OR phone_number ILIKE '%' || pattern || '%';
END;
$$ LANGUAGE plpgsql;
'''

# 3. Жаңа қолданушыны қосу не жаңарту процедурасы
insert_or_update_user_proc = '''
CREATE OR REPLACE PROCEDURE add_or_update_user(
    fname VARCHAR,
    lname VARCHAR,
    pnumber VARCHAR
)
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM phonebook2 WHERE first_name = fname AND last_name = lname) THEN
        UPDATE phonebook2 SET phone_number = pnumber
        WHERE first_name = fname AND last_name = lname;
    ELSE
        INSERT INTO phonebook2(first_name, last_name, phone_number)
        VALUES (fname, lname, pnumber);
    END IF;
END;
$$ LANGUAGE plpgsql;
'''

# 4. Бірнеше қолданушыны қосу процедурасы + телефон тексерісі
insert_many_users_proc = '''
CREATE OR REPLACE PROCEDURE insert_many_users(IN user_list TEXT[][])
LANGUAGE plpgsql
AS $$
DECLARE
    row TEXT[];
    fname TEXT;
    lname TEXT;
    phone TEXT;
    incorrect_data TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOREACH row SLICE 1 IN ARRAY user_list LOOP
        fname := row[1];
        lname := row[2];
        phone := row[3];
        IF phone ~ '^\\+?[0-9\\-\\s]{7,15}$' THEN
            INSERT INTO phonebook2(first_name, last_name, phone_number)
            VALUES (fname, lname, phone);
        ELSE
            incorrect_data := array_append(incorrect_data, fname || ' ' || lname || ' (' || phone || ')');
        END IF;
    END LOOP;
    RAISE NOTICE 'Incorrect data: %', incorrect_data;
END;
$$;
'''

# 5. Пагинация функциясы
pagination_function = '''
CREATE OR REPLACE FUNCTION get_users_with_pagination(limit_count INT, offset_count INT)
RETURNS TABLE(user_id INT, first_name VARCHAR, last_name VARCHAR, phone_number VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM phonebook2
    LIMIT limit_count OFFSET offset_count;
END;
$$ LANGUAGE plpgsql;
'''

# 6. Қолданушыны аты немесе нөмірімен өшіру
delete_user_proc = '''
CREATE OR REPLACE PROCEDURE delete_user(identifier TEXT)
AS $$
BEGIN
    DELETE FROM phonebook2 WHERE first_name = identifier OR phone_number = identifier;
END;
$$ LANGUAGE plpgsql;
'''

queries = [
    create_table_query,
    find_by_pattern_function,
    insert_or_update_user_proc,
    insert_many_users_proc,
    pagination_function,
    delete_user_proc
]

for query in queries:
    execute_query(query)

conn.close()
