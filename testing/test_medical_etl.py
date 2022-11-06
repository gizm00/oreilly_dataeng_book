# cd testing/
# pytest -v test_medical_etl.py -s

def test_create_engine_fixture(test_conn):
    # Example to show population of lookup tables when test_conn fixture is used
    for row in test_conn.execute("SELECT * FROM treatment"):
        print(row)
    for row in test_conn.execute("SELECT * FROM delivery_mechanism"):
        print(row)

def test_match_success(test_conn, match_table, patient_table):
    test_conn.execute("""
        INSERT INTO patient_data VALUES (1, 'Drug A tablet 0.25mg')
    """)

    # For illustration, print out current contents of patient data
    for row in test_conn.execute("SELECT * FROM patient_data"):
        print(row)

    # test matching method ...

def test_match_failure(test_conn, match_table, patient_table):
    test_conn.execute("""
        INSERT INTO patient_data VALUES (10, 'Drug B cap 25mg')
    """)

    # patient_data will have a single row with the data from this test only
    for row in test_conn.execute("SELECT * FROM patient_data"):
        print(row)

    # test matching method ...


